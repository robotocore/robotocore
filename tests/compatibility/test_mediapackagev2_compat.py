"""Compatibility tests for MediaPackage v2 service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def mediapackagev2_client():
    return make_client("mediapackagev2")


@pytest.fixture
def channel_group(mediapackagev2_client):
    name = f"test-cg-{uuid.uuid4().hex[:8]}"
    resp = mediapackagev2_client.create_channel_group(ChannelGroupName=name)
    yield resp
    try:
        mediapackagev2_client.delete_channel_group(ChannelGroupName=name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def channel(mediapackagev2_client, channel_group):
    cg_name = channel_group["ChannelGroupName"]
    ch_name = f"test-ch-{uuid.uuid4().hex[:8]}"
    resp = mediapackagev2_client.create_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
    yield resp
    try:
        mediapackagev2_client.delete_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
    except Exception:
        pass  # best-effort cleanup


class TestMediaPackageV2ChannelGroups:
    def test_create_channel_group(self, mediapackagev2_client):
        name = f"test-cg-{uuid.uuid4().hex[:8]}"
        resp = mediapackagev2_client.create_channel_group(ChannelGroupName=name)
        try:
            assert resp["ChannelGroupName"] == name
            assert "Arn" in resp
            assert "arn:aws:mediapackagev2:" in resp["Arn"]
            assert name in resp["Arn"]
            assert "EgressDomain" in resp
            assert "CreatedAt" in resp
            assert "ModifiedAt" in resp
            assert "ETag" in resp
            assert "Tags" in resp
        finally:
            mediapackagev2_client.delete_channel_group(ChannelGroupName=name)

    def test_get_channel_group(self, mediapackagev2_client, channel_group):
        name = channel_group["ChannelGroupName"]
        resp = mediapackagev2_client.get_channel_group(ChannelGroupName=name)
        assert resp["ChannelGroupName"] == name
        assert resp["Arn"] == channel_group["Arn"]
        assert "EgressDomain" in resp
        assert "CreatedAt" in resp
        assert "ModifiedAt" in resp
        assert "ETag" in resp

    def test_list_channel_groups(self, mediapackagev2_client, channel_group):
        resp = mediapackagev2_client.list_channel_groups()
        assert "Items" in resp
        names = [item["ChannelGroupName"] for item in resp["Items"]]
        assert channel_group["ChannelGroupName"] in names

    def test_delete_channel_group(self, mediapackagev2_client):
        name = f"test-cg-{uuid.uuid4().hex[:8]}"
        mediapackagev2_client.create_channel_group(ChannelGroupName=name)
        mediapackagev2_client.delete_channel_group(ChannelGroupName=name)
        with pytest.raises(Exception) as exc_info:
            mediapackagev2_client.get_channel_group(ChannelGroupName=name)
        assert "ResourceNotFoundException" in str(
            type(exc_info.value).__name__
        ) or "ResourceNotFoundException" in str(exc_info.value)


class TestMediaPackageV2Channels:
    def test_create_channel(self, mediapackagev2_client, channel_group):
        cg_name = channel_group["ChannelGroupName"]
        ch_name = f"test-ch-{uuid.uuid4().hex[:8]}"
        resp = mediapackagev2_client.create_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
        try:
            assert resp["ChannelName"] == ch_name
            assert resp["ChannelGroupName"] == cg_name
            assert "Arn" in resp
            assert "ETag" in resp
        finally:
            mediapackagev2_client.delete_channel(ChannelGroupName=cg_name, ChannelName=ch_name)

    def test_get_channel(self, mediapackagev2_client, channel_group, channel):
        cg_name = channel_group["ChannelGroupName"]
        ch_name = channel["ChannelName"]
        resp = mediapackagev2_client.get_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
        assert resp["ChannelName"] == ch_name
        assert resp["ChannelGroupName"] == cg_name
        assert "Arn" in resp
        assert "ETag" in resp

    def test_delete_channel(self, mediapackagev2_client, channel_group):
        cg_name = channel_group["ChannelGroupName"]
        ch_name = f"test-ch-{uuid.uuid4().hex[:8]}"
        mediapackagev2_client.create_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
        # Should not raise
        mediapackagev2_client.delete_channel(ChannelGroupName=cg_name, ChannelName=ch_name)

    def test_list_channels(self, mediapackagev2_client, channel_group, channel):
        """ListChannels returns channels within a channel group."""
        cg_name = channel_group["ChannelGroupName"]
        resp = mediapackagev2_client.list_channels(ChannelGroupName=cg_name)
        assert "Items" in resp
        names = [item["ChannelName"] for item in resp["Items"]]
        assert channel["ChannelName"] in names

    def test_update_channel(self, mediapackagev2_client, channel_group, channel):
        """UpdateChannel updates a channel's description."""
        cg_name = channel_group["ChannelGroupName"]
        ch_name = channel["ChannelName"]
        resp = mediapackagev2_client.update_channel(
            ChannelGroupName=cg_name,
            ChannelName=ch_name,
            Description="updated description",
        )
        assert resp["ChannelName"] == ch_name
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_channel_group(self, mediapackagev2_client, channel_group):
        """UpdateChannelGroup updates a channel group's description."""
        name = channel_group["ChannelGroupName"]
        resp = mediapackagev2_client.update_channel_group(
            ChannelGroupName=name,
            Description="updated cg description",
        )
        assert resp["ChannelGroupName"] == name
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMediaPackageV2OriginEndpoints:
    @pytest.fixture(autouse=True)
    def setup(self, mediapackagev2_client, channel_group, channel):
        self.client = mediapackagev2_client
        self.cg_name = channel_group["ChannelGroupName"]
        self.ch_name = channel["ChannelName"]

    def _create_endpoint(self, name=None):
        ep_name = name or f"test-ep-{uuid.uuid4().hex[:8]}"
        resp = self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
            ContainerType="TS",
        )
        return resp

    def test_create_origin_endpoint(self):
        """CreateOriginEndpoint creates an endpoint."""
        ep_name = f"test-ep-{uuid.uuid4().hex[:8]}"
        resp = self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
            ContainerType="TS",
        )
        assert resp["OriginEndpointName"] == ep_name
        assert "Arn" in resp
        self.client.delete_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
        )

    def test_get_origin_endpoint(self):
        """GetOriginEndpoint returns endpoint details."""
        ep = self._create_endpoint()
        ep_name = ep["OriginEndpointName"]
        try:
            resp = self.client.get_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )
            assert resp["OriginEndpointName"] == ep_name
            assert "Arn" in resp
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )

    def test_list_origin_endpoints(self):
        """ListOriginEndpoints returns endpoints in a channel."""
        ep = self._create_endpoint()
        ep_name = ep["OriginEndpointName"]
        try:
            resp = self.client.list_origin_endpoints(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
            )
            assert "Items" in resp
            names = [item["OriginEndpointName"] for item in resp["Items"]]
            assert ep_name in names
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )

    def test_delete_origin_endpoint(self):
        """DeleteOriginEndpoint removes an endpoint."""
        ep = self._create_endpoint()
        ep_name = ep["OriginEndpointName"]
        resp = self.client.delete_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_origin_endpoint(self):
        """UpdateOriginEndpoint updates an endpoint."""
        ep = self._create_endpoint()
        ep_name = ep["OriginEndpointName"]
        try:
            resp = self.client.update_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
                ContainerType="TS",
                Description="updated endpoint",
            )
            assert resp["OriginEndpointName"] == ep_name
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )


class TestMediaPackageV2Policies:
    @pytest.fixture(autouse=True)
    def setup(self, mediapackagev2_client, channel_group, channel):
        self.client = mediapackagev2_client
        self.cg_name = channel_group["ChannelGroupName"]
        self.ch_name = channel["ChannelName"]

    def test_put_channel_policy(self):
        """PutChannelPolicy sets a channel policy."""
        policy = '{"Version":"2012-10-17","Statement":[]}'
        resp = self.client.put_channel_policy(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            Policy=policy,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_channel_policy(self):
        """GetChannelPolicy retrieves a channel policy."""
        policy = '{"Version":"2012-10-17","Statement":[]}'
        self.client.put_channel_policy(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            Policy=policy,
        )
        resp = self.client.get_channel_policy(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
        )
        assert "Policy" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_channel_policy(self):
        """DeleteChannelPolicy removes a channel policy."""
        policy = '{"Version":"2012-10-17","Statement":[]}'
        self.client.put_channel_policy(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            Policy=policy,
        )
        resp = self.client.delete_channel_policy(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_origin_endpoint_policy(self):
        """PutOriginEndpointPolicy sets a policy on an endpoint."""
        ep_name = f"test-ep-{uuid.uuid4().hex[:8]}"
        self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
            ContainerType="TS",
        )
        try:
            policy = '{"Version":"2012-10-17","Statement":[]}'
            resp = self.client.put_origin_endpoint_policy(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
                Policy=policy,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )

    def test_get_origin_endpoint_policy(self):
        """GetOriginEndpointPolicy retrieves a policy."""
        ep_name = f"test-ep-{uuid.uuid4().hex[:8]}"
        self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
            ContainerType="TS",
        )
        try:
            policy = '{"Version":"2012-10-17","Statement":[]}'
            self.client.put_origin_endpoint_policy(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
                Policy=policy,
            )
            resp = self.client.get_origin_endpoint_policy(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )
            assert "Policy" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )

    def test_delete_origin_endpoint_policy(self):
        """DeleteOriginEndpointPolicy removes a policy."""
        ep_name = f"test-ep-{uuid.uuid4().hex[:8]}"
        self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
            ContainerType="TS",
        )
        try:
            policy = '{"Version":"2012-10-17","Statement":[]}'
            self.client.put_origin_endpoint_policy(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
                Policy=policy,
            )
            resp = self.client.delete_origin_endpoint_policy(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )


class TestMediaPackageV2Tags:
    def test_tag_resource(self, mediapackagev2_client, channel_group):
        """TagResource adds tags to a resource."""
        arn = channel_group["Arn"]
        resp = mediapackagev2_client.tag_resource(
            ResourceArn=arn,
            Tags={"env": "test", "project": "robotocore"},
        )
        assert (
            resp["ResponseMetadata"]["HTTPStatusCode"] == 204
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )

    def test_list_tags_for_resource(self, mediapackagev2_client, channel_group):
        """ListTagsForResource returns tags on a resource."""
        arn = channel_group["Arn"]
        resp = mediapackagev2_client.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], dict)

    def test_untag_resource(self, mediapackagev2_client, channel_group):
        """UntagResource removes tags from a resource."""
        arn = channel_group["Arn"]
        mediapackagev2_client.tag_resource(
            ResourceArn=arn,
            Tags={"env": "test"},
        )
        resp = mediapackagev2_client.untag_resource(
            ResourceArn=arn,
            TagKeys=["env"],
        )
        assert (
            resp["ResponseMetadata"]["HTTPStatusCode"] == 204
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )


class TestMediaPackageV2StateReset:
    @pytest.fixture(autouse=True)
    def setup(self, mediapackagev2_client, channel_group, channel):
        self.client = mediapackagev2_client
        self.cg_name = channel_group["ChannelGroupName"]
        self.ch_name = channel["ChannelName"]

    def test_reset_channel_state(self):
        """ResetChannelState resets a channel's state."""
        resp = self.client.reset_channel_state(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_reset_origin_endpoint_state(self):
        """ResetOriginEndpointState resets an endpoint's state."""
        ep_name = f"test-ep-{uuid.uuid4().hex[:8]}"
        self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=ep_name,
            ContainerType="TS",
        )
        try:
            resp = self.client.reset_origin_endpoint_state(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=ep_name,
            )


class TestMediaPackageV2HarvestJobs:
    @pytest.fixture(autouse=True)
    def setup(self, mediapackagev2_client, channel_group, channel):
        self.client = mediapackagev2_client
        self.cg_name = channel_group["ChannelGroupName"]
        self.ch_name = channel["ChannelName"]
        # Create an origin endpoint for harvest jobs
        self.ep_name = f"test-ep-{uuid.uuid4().hex[:8]}"
        self.client.create_origin_endpoint(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
            ContainerType="TS",
        )
        yield
        try:
            self.client.delete_origin_endpoint(
                ChannelGroupName=self.cg_name,
                ChannelName=self.ch_name,
                OriginEndpointName=self.ep_name,
            )
        except Exception:
            pass  # best-effort cleanup

    def test_list_harvest_jobs(self):
        """ListHarvestJobs returns a list of harvest jobs."""
        resp = self.client.list_harvest_jobs(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
        )
        assert "Items" in resp
        assert isinstance(resp["Items"], list)

    def test_create_harvest_job(self):
        """CreateHarvestJob creates a new harvest job."""
        resp = self.client.create_harvest_job(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
            HarvestedManifests={
                "HlsManifests": [
                    {"ManifestName": "index"},
                ],
            },
            Destination={
                "S3Destination": {
                    "BucketName": "test-bucket",
                    "DestinationPath": "harvests/",
                },
            },
            ScheduleConfiguration={
                "StartTime": "2026-01-01T00:00:00Z",
                "EndTime": "2026-01-02T00:00:00Z",
            },
        )
        assert "HarvestJobName" in resp or "Arn" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_harvest_job(self):
        """GetHarvestJob retrieves a specific harvest job."""
        create_resp = self.client.create_harvest_job(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
            HarvestedManifests={
                "HlsManifests": [
                    {"ManifestName": "index"},
                ],
            },
            Destination={
                "S3Destination": {
                    "BucketName": "test-bucket",
                    "DestinationPath": "harvests/",
                },
            },
            ScheduleConfiguration={
                "StartTime": "2026-01-01T00:00:00Z",
                "EndTime": "2026-01-02T00:00:00Z",
            },
        )
        job_name = create_resp["HarvestJobName"]
        resp = self.client.get_harvest_job(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
            HarvestJobName=job_name,
        )
        assert resp["HarvestJobName"] == job_name
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_harvest_job(self):
        """CancelHarvestJob cancels a harvest job."""
        create_resp = self.client.create_harvest_job(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
            HarvestedManifests={
                "HlsManifests": [
                    {"ManifestName": "index"},
                ],
            },
            Destination={
                "S3Destination": {
                    "BucketName": "test-bucket",
                    "DestinationPath": "harvests/",
                },
            },
            ScheduleConfiguration={
                "StartTime": "2026-01-01T00:00:00Z",
                "EndTime": "2026-01-02T00:00:00Z",
            },
        )
        job_name = create_resp["HarvestJobName"]
        resp = self.client.cancel_harvest_job(
            ChannelGroupName=self.cg_name,
            ChannelName=self.ch_name,
            OriginEndpointName=self.ep_name,
            HarvestJobName=job_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
