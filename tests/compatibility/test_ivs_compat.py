"""IVS (Interactive Video Service) compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def ivs():
    return make_client("ivs")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestIVSChannelOperations:
    """Tests for IVS channel CRUD operations."""

    def test_create_channel_defaults(self, ivs):
        """Create a channel with default settings."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel = resp["channel"]
        try:
            assert channel["name"] == name
            assert "arn" in channel
            assert channel["arn"].startswith("arn:aws:ivs:")
            assert channel["latencyMode"] == "LOW"
            assert channel["type"] == "STANDARD"
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_create_channel_returns_stream_key(self, ivs):
        """create_channel also returns a streamKey."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            sk = resp["streamKey"]
            assert "arn" in sk
            assert sk["channelArn"] == channel_arn
            assert "value" in sk
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_create_channel_custom_params(self, ivs):
        """Create a channel with custom latencyMode and type."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, latencyMode="NORMAL", type="BASIC")
        channel = resp["channel"]
        try:
            assert channel["latencyMode"] == "NORMAL"
            assert channel["type"] == "BASIC"
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_create_channel_with_tags(self, ivs):
        """Create a channel with tags included at creation time."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"env": "test", "team": "dev"})
        channel = resp["channel"]
        try:
            assert channel["tags"]["env"] == "test"
            assert channel["tags"]["team"] == "dev"
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_get_channel(self, ivs):
        """Get a channel by ARN."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["name"] == name
            assert got["channel"]["arn"] == channel_arn
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_get_channel_not_found(self, ivs):
        """get_channel raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:channel/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_channels(self, ivs):
        """list_channels returns created channels."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            listed = ivs.list_channels()
            arns = [ch["arn"] for ch in listed["channels"]]
            assert channel_arn in arns
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_update_channel(self, ivs):
        """update_channel modifies channel properties."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            updated_name = _unique("ch-upd")
            upd = ivs.update_channel(arn=channel_arn, name=updated_name)
            assert upd["channel"]["name"] == updated_name
            # Verify via get
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["name"] == updated_name
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_delete_channel(self, ivs):
        """delete_channel removes the channel."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        ivs.delete_channel(arn=channel_arn)
        # Verify it's gone
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=channel_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_get_channel(self, ivs):
        """batch_get_channel retrieves multiple channels at once."""
        name1 = _unique("ch")
        name2 = _unique("ch")
        resp1 = ivs.create_channel(name=name1)
        resp2 = ivs.create_channel(name=name2)
        arn1 = resp1["channel"]["arn"]
        arn2 = resp2["channel"]["arn"]
        try:
            batch = ivs.batch_get_channel(arns=[arn1, arn2])
            returned_arns = {ch["arn"] for ch in batch["channels"]}
            assert arn1 in returned_arns
            assert arn2 in returned_arns
        finally:
            ivs.delete_channel(arn=arn1)
            ivs.delete_channel(arn=arn2)
