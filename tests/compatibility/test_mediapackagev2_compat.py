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
        pass


@pytest.fixture
def channel(mediapackagev2_client, channel_group):
    cg_name = channel_group["ChannelGroupName"]
    ch_name = f"test-ch-{uuid.uuid4().hex[:8]}"
    resp = mediapackagev2_client.create_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
    yield resp
    try:
        mediapackagev2_client.delete_channel(ChannelGroupName=cg_name, ChannelName=ch_name)
    except Exception:
        pass


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
