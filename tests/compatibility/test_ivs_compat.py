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

    def test_list_channels_filter_by_recording_config(self, ivs):
        """list_channels with filterByRecordingConfigurationArn returns channels key."""
        resp = ivs.list_channels(filterByRecordingConfigurationArn="")
        assert "channels" in resp
        assert isinstance(resp["channels"], list)


class TestIVSStreamKeyOperations:
    """Tests for IVS stream key CRUD operations."""

    def test_create_stream_key(self, ivs):
        """create_stream_key creates a new stream key for a channel."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn)
            stream_key = sk["streamKey"]
            assert "arn" in stream_key
            assert stream_key["channelArn"] == ch_arn
            assert "value" in stream_key
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_get_stream_key(self, ivs):
        """get_stream_key retrieves a stream key by ARN."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn)
            sk_arn = sk["streamKey"]["arn"]
            got = ivs.get_stream_key(arn=sk_arn)
            assert got["streamKey"]["arn"] == sk_arn
            assert got["streamKey"]["channelArn"] == ch_arn
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_list_stream_keys(self, ivs):
        """list_stream_keys returns stream keys for a channel."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            # Channel creation also creates a stream key
            listed = ivs.list_stream_keys(channelArn=ch_arn)
            assert "streamKeys" in listed
            assert len(listed["streamKeys"]) >= 1
            assert all(sk["channelArn"] == ch_arn for sk in listed["streamKeys"])
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_delete_stream_key(self, ivs):
        """delete_stream_key removes a stream key."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn)
            sk_arn = sk["streamKey"]["arn"]
            ivs.delete_stream_key(arn=sk_arn)
            # Verify deleted
            with pytest.raises(ClientError) as exc_info:
                ivs.get_stream_key(arn=sk_arn)
            assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            ivs.delete_channel(arn=ch_arn)


class TestIVSRecordingConfigurationOperations:
    """Tests for IVS recording configuration operations."""

    def test_create_recording_configuration(self, ivs):
        """create_recording_configuration creates a config."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-bucket-ivs-001"}}
        )
        rc = resp["recordingConfiguration"]
        try:
            assert "arn" in rc
            assert rc["arn"].startswith("arn:aws:ivs:")
            assert "state" in rc
            assert "destinationConfiguration" in rc
        finally:
            ivs.delete_recording_configuration(arn=rc["arn"])

    def test_get_recording_configuration(self, ivs):
        """get_recording_configuration retrieves a config by ARN."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-bucket-ivs-002"}}
        )
        rc_arn = resp["recordingConfiguration"]["arn"]
        try:
            got = ivs.get_recording_configuration(arn=rc_arn)
            assert got["recordingConfiguration"]["arn"] == rc_arn
            s3_cfg = got["recordingConfiguration"]["destinationConfiguration"]["s3"]
            assert s3_cfg["bucketName"] == "test-bucket-ivs-002"
        finally:
            ivs.delete_recording_configuration(arn=rc_arn)

    def test_list_recording_configurations(self, ivs):
        """list_recording_configurations returns config list."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-bucket-ivs-003"}}
        )
        rc_arn = resp["recordingConfiguration"]["arn"]
        try:
            listed = ivs.list_recording_configurations()
            assert "recordingConfigurations" in listed
            arns = [rc["arn"] for rc in listed["recordingConfigurations"]]
            assert rc_arn in arns
        finally:
            ivs.delete_recording_configuration(arn=rc_arn)

    def test_delete_recording_configuration(self, ivs):
        """delete_recording_configuration removes a config."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-bucket-ivs-004"}}
        )
        rc_arn = resp["recordingConfiguration"]["arn"]
        ivs.delete_recording_configuration(arn=rc_arn)
        with pytest.raises(ClientError) as exc_info:
            ivs.get_recording_configuration(arn=rc_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIVSPlaybackKeyPairOperations:
    """Tests for IVS playback key pair CRUD and error handling."""

    _PUB_KEY = (
        "-----BEGIN PUBLIC KEY-----\n"
        "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEFake1234567890ABCDEFGHIJKLMN"
        "OPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890+/=\n"
        "-----END PUBLIC KEY-----"
    )

    def test_import_playback_key_pair(self, ivs):
        """import_playback_key_pair creates a key pair and returns its details."""
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=_unique("kp"),
        )
        kp = resp["keyPair"]
        try:
            assert "arn" in kp
            assert kp["arn"].startswith("arn:aws:ivs:")
            assert "fingerprint" in kp
        finally:
            ivs.delete_playback_key_pair(arn=kp["arn"])

    def test_get_playback_key_pair(self, ivs):
        """get_playback_key_pair retrieves an imported key pair by ARN."""
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=_unique("kp"),
        )
        kp_arn = resp["keyPair"]["arn"]
        try:
            got = ivs.get_playback_key_pair(arn=kp_arn)
            assert got["keyPair"]["arn"] == kp_arn
            assert "fingerprint" in got["keyPair"]
        finally:
            ivs.delete_playback_key_pair(arn=kp_arn)

    def test_delete_playback_key_pair(self, ivs):
        """delete_playback_key_pair removes an imported key pair."""
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=_unique("kp"),
        )
        kp_arn = resp["keyPair"]["arn"]
        ivs.delete_playback_key_pair(arn=kp_arn)
        with pytest.raises(ClientError) as exc_info:
            ivs.get_playback_key_pair(arn=kp_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_playback_key_pair_not_found(self, ivs):
        """get_playback_key_pair raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:playback-key/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.get_playback_key_pair(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_playback_key_pair_not_found(self, ivs):
        """delete_playback_key_pair raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:playback-key/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.delete_playback_key_pair(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIVSPlaybackKeys:
    """Tests for IVS playback key pair list operations."""

    def test_list_playback_key_pairs(self, ivs):
        """list_playback_key_pairs returns the expected key."""
        resp = ivs.list_playback_key_pairs()
        assert "keyPairs" in resp
        assert isinstance(resp["keyPairs"], list)


class TestIVSTags:
    """Tests for IVS tagging operations."""

    def test_list_tags_for_channel(self, ivs):
        """Create a channel and list its tags."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"env": "test"})
        channel_arn = resp["channel"]["arn"]
        try:
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert "tags" in tags_resp
            assert tags_resp["tags"]["env"] == "test"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_tag_resource_on_channel(self, ivs):
        """Create a channel, then tag it with tag_resource."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            ivs.tag_resource(resourceArn=channel_arn, tags={"team": "dev", "stage": "qa"})
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert tags_resp["tags"]["team"] == "dev"
            assert tags_resp["tags"]["stage"] == "qa"
        finally:
            ivs.delete_channel(arn=channel_arn)


class TestIVSStreamKeyBatchOperations:
    """Tests for IVS batch stream key operations."""

    def test_batch_get_stream_key(self, ivs):
        """batch_get_stream_key retrieves multiple stream keys at once."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk1 = ivs.create_stream_key(channelArn=ch_arn)
            sk2 = ivs.create_stream_key(channelArn=ch_arn)
            sk1_arn = sk1["streamKey"]["arn"]
            sk2_arn = sk2["streamKey"]["arn"]
            batch = ivs.batch_get_stream_key(arns=[sk1_arn, sk2_arn])
            assert "streamKeys" in batch
            returned_arns = {sk["arn"] for sk in batch["streamKeys"]}
            assert sk1_arn in returned_arns
            assert sk2_arn in returned_arns
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_untag_resource_not_found(self, ivs):
        """untag_resource raises ResourceNotFoundException for missing ARN."""
        fake = "arn:aws:ivs:us-east-1:123456789012:channel/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.untag_resource(resourceArn=fake, tagKeys=["env"])
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_get_stream_key_with_errors(self, ivs):
        """batch_get_stream_key returns errors for nonexistent keys."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:stream-key/nonexistent"
        batch = ivs.batch_get_stream_key(arns=[fake_arn])
        assert "errors" in batch
        assert "streamKeys" in batch
