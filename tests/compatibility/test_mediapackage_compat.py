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

    def test_list_origin_endpoints_by_channel(self, mediapackage):
        channel_id = _unique_id("ch")
        ep1 = _unique_id("ep")
        ep2 = _unique_id("ep")
        mediapackage.create_channel(Id=channel_id)
        mediapackage.create_origin_endpoint(
            ChannelId=channel_id, Id=ep1, HlsPackage={}
        )
        mediapackage.create_origin_endpoint(
            ChannelId=channel_id, Id=ep2, HlsPackage={}
        )
        response = mediapackage.list_origin_endpoints(ChannelId=channel_id)
        endpoint_ids = [ep["Id"] for ep in response["OriginEndpoints"]]
        assert ep1 in endpoint_ids
        assert ep2 in endpoint_ids
