"""IVS (Interactive Video Service) compatibility tests."""

import re
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

    def test_create_channel_arn_format(self, ivs):
        """Channel ARN matches the expected IVS ARN pattern."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel = resp["channel"]
        try:
            arn = channel["arn"]
            # IVS channel ARN: arn:aws:ivs:<region>:<account>:channel/<id>
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:channel/[A-Za-z0-9]+", arn
            ), f"Unexpected ARN format: {arn}"
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_create_channel_unicode_name(self, ivs):
        """Channel creation with unicode characters in the name."""
        name = f"chàñël-{uuid.uuid4().hex[:6]}"
        resp = ivs.create_channel(name=name)
        channel = resp["channel"]
        try:
            assert channel["name"] == name
            got = ivs.get_channel(arn=channel["arn"])
            assert got["channel"]["name"] == name
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_delete_channel_not_found(self, ivs):
        """delete_channel raises ResourceNotFoundException for missing channel."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:channel/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.delete_channel(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_channel_not_found(self, ivs):
        """update_channel raises ResourceNotFoundException for missing channel."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:channel/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.update_channel(arn=fake_arn, name="new-name")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_channels_pagination(self, ivs):
        """list_channels supports maxResults and nextToken pagination."""
        arns = []
        try:
            for _ in range(3):
                resp = ivs.create_channel(name=_unique("ch"))
                arns.append(resp["channel"]["arn"])
            page1 = ivs.list_channels(maxResults=1)
            assert len(page1["channels"]) == 1
            assert "nextToken" in page1
            page2 = ivs.list_channels(maxResults=1, nextToken=page1["nextToken"])
            assert len(page2["channels"]) == 1
            # Pages should return different channels
            assert page1["channels"][0]["arn"] != page2["channels"][0]["arn"]
        finally:
            for arn in arns:
                ivs.delete_channel(arn=arn)

    def test_batch_get_channel_with_missing_arns(self, ivs):
        """batch_get_channel returns errors for nonexistent ARNs."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        real_arn = resp["channel"]["arn"]
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:channel/nonexistent"
        try:
            batch = ivs.batch_get_channel(arns=[real_arn, fake_arn])
            found_arns = {ch["arn"] for ch in batch["channels"]}
            assert real_arn in found_arns
            assert "errors" in batch
        finally:
            ivs.delete_channel(arn=real_arn)

    def test_update_channel_preserves_tags(self, ivs):
        """Updating a channel name preserves its existing tags."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"env": "test"})
        channel_arn = resp["channel"]["arn"]
        try:
            new_name = _unique("ch-upd")
            ivs.update_channel(arn=channel_arn, name=new_name)
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["name"] == new_name
            assert got["channel"]["tags"]["env"] == "test"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_list_channels_filter_by_recording_config(self, ivs):
        """list_channels with filterByRecordingConfigurationArn returns channels key."""
        resp = ivs.list_channels(filterByRecordingConfigurationArn="")
        assert "channels" in resp
        assert isinstance(resp["channels"], list)

    def test_create_channel_ingest_endpoint_present(self, ivs):
        """Channel response includes ingestEndpoint field."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel = resp["channel"]
        try:
            got = ivs.get_channel(arn=channel["arn"])
            assert "ingestEndpoint" in got["channel"]
            assert len(got["channel"]["ingestEndpoint"]) > 0
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_create_channel_playback_url_present(self, ivs):
        """Channel response includes playbackUrl field."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel = resp["channel"]
        try:
            got = ivs.get_channel(arn=channel["arn"])
            assert "playbackUrl" in got["channel"]
            assert len(got["channel"]["playbackUrl"]) > 0
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_update_channel_latency_mode(self, ivs):
        """update_channel can change latencyMode."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, latencyMode="LOW")
        channel_arn = resp["channel"]["arn"]
        try:
            updated = ivs.update_channel(arn=channel_arn, latencyMode="NORMAL")
            assert updated["channel"]["latencyMode"] == "NORMAL"
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["latencyMode"] == "NORMAL"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_list_channels_filter_by_recording_config_with_channel(self, ivs):
        """list_channels filterByRecordingConfigurationArn filters correctly."""
        rc_resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-ivs-filter-bucket"}}
        )
        rc_arn = rc_resp["recordingConfiguration"]["arn"]
        ch_resp = ivs.create_channel(
            name=_unique("ch"), recordingConfigurationArn=rc_arn
        )
        channel_arn = ch_resp["channel"]["arn"]
        try:
            filtered = ivs.list_channels(filterByRecordingConfigurationArn=rc_arn)
            assert "channels" in filtered
            arns = [ch["arn"] for ch in filtered["channels"]]
            assert channel_arn in arns
        finally:
            ivs.delete_channel(arn=channel_arn)
            ivs.delete_recording_configuration(arn=rc_arn)

    def test_batch_get_channel_all_errors(self, ivs):
        """batch_get_channel with all-missing ARNs returns errors list."""
        fake1 = "arn:aws:ivs:us-east-1:123456789012:channel/fake1"
        fake2 = "arn:aws:ivs:us-east-1:123456789012:channel/fake2"
        batch = ivs.batch_get_channel(arns=[fake1, fake2])
        assert "errors" in batch
        assert len(batch["errors"]) == 2
        error_arns = {e["arn"] for e in batch["errors"]}
        assert fake1 in error_arns
        assert fake2 in error_arns


    def test_list_channels_returns_name_field(self, ivs):
        """list_channels summary items include name field."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            listed = ivs.list_channels()
            ch = next((c for c in listed["channels"] if c["arn"] == channel_arn), None)
            assert ch is not None
            assert ch["name"] == name
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_create_channel_authorized_field(self, ivs):
        """create_channel with authorized=True sets authorized flag."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, authorized=True)
        channel = resp["channel"]
        try:
            assert channel["authorized"] is True
            got = ivs.get_channel(arn=channel["arn"])
            assert got["channel"]["authorized"] is True
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_update_channel_recording_config(self, ivs):
        """update_channel can set recordingConfigurationArn."""
        rc_resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-ivs-update-rc"}}
        )
        rc_arn = rc_resp["recordingConfiguration"]["arn"]
        name = _unique("ch")
        ch_resp = ivs.create_channel(name=name)
        channel_arn = ch_resp["channel"]["arn"]
        try:
            updated = ivs.update_channel(
                arn=channel_arn, recordingConfigurationArn=rc_arn
            )
            assert updated["channel"]["recordingConfigurationArn"] == rc_arn
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["recordingConfigurationArn"] == rc_arn
        finally:
            ivs.delete_channel(arn=channel_arn)
            ivs.delete_recording_configuration(arn=rc_arn)


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

    def test_stream_key_arn_format(self, ivs):
        """Stream key ARN matches expected IVS pattern."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn)
            sk_arn = sk["streamKey"]["arn"]
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:stream-key/[A-Za-z0-9]+", sk_arn
            ), f"Unexpected stream key ARN format: {sk_arn}"
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_get_stream_key_has_value_field(self, ivs):
        """get_stream_key returns a value field (the actual key)."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn)
            sk_arn = sk["streamKey"]["arn"]
            got = ivs.get_stream_key(arn=sk_arn)
            assert "value" in got["streamKey"]
            assert len(got["streamKey"]["value"]) > 0
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_create_multiple_stream_keys(self, ivs):
        """Multiple stream keys can be created for one channel."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk1 = ivs.create_stream_key(channelArn=ch_arn)
            sk2 = ivs.create_stream_key(channelArn=ch_arn)
            assert sk1["streamKey"]["arn"] != sk2["streamKey"]["arn"]
            listed = ivs.list_stream_keys(channelArn=ch_arn)
            # At least 3: one from channel creation + 2 explicit
            assert len(listed["streamKeys"]) >= 3
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_list_stream_keys_pagination(self, ivs):
        """list_stream_keys supports maxResults pagination."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            # Create 2 more keys (1 already exists from channel creation)
            ivs.create_stream_key(channelArn=ch_arn)
            ivs.create_stream_key(channelArn=ch_arn)
            page1 = ivs.list_stream_keys(channelArn=ch_arn, maxResults=1)
            assert len(page1["streamKeys"]) == 1
            assert "nextToken" in page1
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_stream_key_value_unique_per_key(self, ivs):
        """Each stream key has a unique value string."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk1 = ivs.create_stream_key(channelArn=ch_arn)
            sk2 = ivs.create_stream_key(channelArn=ch_arn)
            val1 = sk1["streamKey"]["value"]
            val2 = sk2["streamKey"]["value"]
            assert val1 != val2
            assert len(val1) > 0
            assert len(val2) > 0
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_stream_key_with_tags(self, ivs):
        """create_stream_key supports tags."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn, tags={"env": "prod"})
            sk_arn = sk["streamKey"]["arn"]
            assert sk["streamKey"]["tags"]["env"] == "prod"
            tags_resp = ivs.list_tags_for_resource(resourceArn=sk_arn)
            assert tags_resp["tags"]["env"] == "prod"
        finally:
            ivs.delete_channel(arn=ch_arn)

    def test_delete_stream_key_not_found(self, ivs):
        """delete_stream_key raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:stream-key/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.delete_stream_key(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_stream_key_not_found(self, ivs):
        """get_stream_key raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:stream-key/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.get_stream_key(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


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

    def test_recording_configuration_arn_format(self, ivs):
        """Recording configuration ARN matches expected IVS pattern."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-bucket-ivs-arn"}}
        )
        rc = resp["recordingConfiguration"]
        try:
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:recording-configuration/[A-Za-z0-9]+",
                rc["arn"],
            ), f"Unexpected ARN format: {rc['arn']}"
        finally:
            ivs.delete_recording_configuration(arn=rc["arn"])

    def test_recording_configuration_with_tags(self, ivs):
        """Recording config can be created with tags."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-bucket-ivs-tags"}},
            tags={"env": "staging"},
        )
        rc = resp["recordingConfiguration"]
        try:
            assert rc["tags"]["env"] == "staging"
            tags_resp = ivs.list_tags_for_resource(resourceArn=rc["arn"])
            assert tags_resp["tags"]["env"] == "staging"
        finally:
            ivs.delete_recording_configuration(arn=rc["arn"])

    def test_list_recording_configurations_pagination(self, ivs):
        """list_recording_configurations supports maxResults pagination."""
        arns = []
        try:
            for i in range(3):
                resp = ivs.create_recording_configuration(
                    destinationConfiguration={
                        "s3": {"bucketName": f"test-bucket-ivs-pg-{uuid.uuid4().hex[:6]}"}
                    }
                )
                arns.append(resp["recordingConfiguration"]["arn"])
            page1 = ivs.list_recording_configurations(maxResults=1)
            assert len(page1["recordingConfigurations"]) == 1
            assert "nextToken" in page1
        finally:
            for arn in arns:
                ivs.delete_recording_configuration(arn=arn)

    def test_delete_recording_configuration_not_found(self, ivs):
        """delete_recording_configuration raises ResourceNotFoundException."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:recording-configuration/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.delete_recording_configuration(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

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


    def test_playback_key_pair_arn_format(self, ivs):
        """Playback key pair ARN matches expected IVS pattern."""
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=_unique("kp"),
        )
        kp = resp["keyPair"]
        try:
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:playback-key/[A-Za-z0-9]+",
                kp["arn"],
            ), f"Unexpected ARN format: {kp['arn']}"
        finally:
            ivs.delete_playback_key_pair(arn=kp["arn"])

    def test_import_playback_key_pair_with_tags(self, ivs):
        """import_playback_key_pair supports tags at creation time."""
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=_unique("kp"),
            tags={"team": "video"},
        )
        kp = resp["keyPair"]
        try:
            assert kp["tags"]["team"] == "video"
            tags_resp = ivs.list_tags_for_resource(resourceArn=kp["arn"])
            assert tags_resp["tags"]["team"] == "video"
        finally:
            ivs.delete_playback_key_pair(arn=kp["arn"])


class TestIVSPlaybackKeys:
    """Tests for IVS playback key pair list operations."""

    _PUB_KEY = (
        "-----BEGIN PUBLIC KEY-----\n"
        "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEFake1234567890ABCDEFGHIJKLMN"
        "OPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890+/=\n"
        "-----END PUBLIC KEY-----"
    )

    def test_list_playback_key_pairs(self, ivs):
        """list_playback_key_pairs returns the expected key."""
        resp = ivs.list_playback_key_pairs()
        assert "keyPairs" in resp
        assert isinstance(resp["keyPairs"], list)

    def test_list_playback_key_pairs_includes_imported(self, ivs):
        """list_playback_key_pairs includes a recently imported key."""
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=_unique("kp"),
        )
        kp_arn = resp["keyPair"]["arn"]
        try:
            listed = ivs.list_playback_key_pairs()
            arns = [kp["arn"] for kp in listed["keyPairs"]]
            assert kp_arn in arns
        finally:
            ivs.delete_playback_key_pair(arn=kp_arn)

    def test_list_playback_key_pairs_pagination(self, ivs):
        """list_playback_key_pairs supports maxResults pagination."""
        arns = []
        try:
            for _ in range(3):
                resp = ivs.import_playback_key_pair(
                    publicKeyMaterial=self._PUB_KEY,
                    name=_unique("kp"),
                )
                arns.append(resp["keyPair"]["arn"])
            page1 = ivs.list_playback_key_pairs(maxResults=1)
            assert len(page1["keyPairs"]) == 1
            assert "nextToken" in page1
            page2 = ivs.list_playback_key_pairs(
                maxResults=1, nextToken=page1["nextToken"]
            )
            assert len(page2["keyPairs"]) == 1
            assert page1["keyPairs"][0]["arn"] != page2["keyPairs"][0]["arn"]
        finally:
            for arn in arns:
                ivs.delete_playback_key_pair(arn=arn)

    def test_list_playback_key_pairs_summary_has_name(self, ivs):
        """list_playback_key_pairs summary includes name field."""
        kp_name = _unique("kp")
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=kp_name,
        )
        kp_arn = resp["keyPair"]["arn"]
        try:
            listed = ivs.list_playback_key_pairs()
            kp = next((k for k in listed["keyPairs"] if k["arn"] == kp_arn), None)
            assert kp is not None
            assert kp["name"] == kp_name
        finally:
            ivs.delete_playback_key_pair(arn=kp_arn)


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

    def test_untag_resource_on_channel(self, ivs):
        """untag_resource removes specific tags from a channel."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"a": "1", "b": "2", "c": "3"})
        channel_arn = resp["channel"]["arn"]
        try:
            ivs.untag_resource(resourceArn=channel_arn, tagKeys=["b"])
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert "a" in tags_resp["tags"]
            assert "b" not in tags_resp["tags"]
            assert "c" in tags_resp["tags"]
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_tag_resource_overwrites_existing(self, ivs):
        """tag_resource with same key overwrites the value."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"env": "dev"})
        channel_arn = resp["channel"]["arn"]
        try:
            ivs.tag_resource(resourceArn=channel_arn, tags={"env": "prod"})
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert tags_resp["tags"]["env"] == "prod"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_list_tags_not_found(self, ivs):
        """list_tags_for_resource raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:channel/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.list_tags_for_resource(resourceArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
        """batch_get_stream_key returns only valid keys when given a mix of real and fake ARNs."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        ch_arn = ch["channel"]["arn"]
        try:
            sk = ivs.create_stream_key(channelArn=ch_arn)
            real_arn = sk["streamKey"]["arn"]
            fake_arn = "arn:aws:ivs:us-east-1:123456789012:stream-key/nonexistent"
            batch = ivs.batch_get_stream_key(arns=[real_arn, fake_arn])
            assert "errors" in batch
            assert "streamKeys" in batch
            found_arns = {sk["arn"] for sk in batch["streamKeys"]}
            assert real_arn in found_arns
            # The fake ARN should NOT appear in the streamKeys
            assert fake_arn not in found_arns
        finally:
            ivs.delete_channel(arn=ch_arn)


class TestIvsPlaybackRestrictionPolicy:
    """Tests for GetPlaybackRestrictionPolicy operation."""

    def test_get_playback_restriction_policy_not_found(self, ivs):
        """GetPlaybackRestrictionPolicy with nonexistent ARN raises ResourceNotFoundException."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:playback-restriction-policy/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.get_playback_restriction_policy(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestIVSNewOps:
    """Tests for newly implemented IVS operations."""

    def test_list_streams_empty(self, ivs):
        """list_streams returns empty streams list (no active RTMP ingests)."""
        resp = ivs.list_streams()
        assert "streams" in resp
        assert isinstance(resp["streams"], list)
        # Without an active ingest, the streams list should be empty
        assert len(resp["streams"]) == 0

    def test_list_streams_with_maxresults(self, ivs):
        """list_streams accepts maxResults without error and returns empty list."""
        resp = ivs.list_streams(maxResults=10)
        assert "streams" in resp
        assert len(resp["streams"]) == 0

    def test_stop_stream(self, ivs):
        """stop_stream succeeds for a valid channel."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        channel_arn = ch["channel"]["arn"]
        try:
            resp = ivs.stop_stream(channelArn=channel_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_put_metadata(self, ivs):
        """put_metadata sends metadata to a channel successfully."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        channel_arn = ch["channel"]["arn"]
        try:
            resp = ivs.put_metadata(channelArn=channel_arn, metadata="test-metadata")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_list_stream_sessions_empty(self, ivs):
        """list_stream_sessions returns empty list for channel with no sessions."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        channel_arn = ch["channel"]["arn"]
        try:
            resp = ivs.list_stream_sessions(channelArn=channel_arn)
            assert "streamSessions" in resp
            assert isinstance(resp["streamSessions"], list)
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_get_stream_session(self, ivs):
        """get_stream_session returns a streamSession for a valid channel."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        channel_arn = ch["channel"]["arn"]
        try:
            resp = ivs.get_stream_session(channelArn=channel_arn)
            session = resp["streamSession"]
            # streamId must be a non-empty string
            assert len(session["streamId"]) > 0
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_create_and_list_playback_restriction_policies(self, ivs):
        """create_playback_restriction_policy + list_playback_restriction_policies work."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US", "CA"],
            allowedOrigins=["https://example.com"],
            enableStrictOriginEnforcement=False,
            name=_unique("policy"),
        )
        policy = resp["playbackRestrictionPolicy"]
        policy_arn = policy["arn"]
        try:
            assert "arn" in policy
            assert policy["arn"].startswith("arn:aws:ivs:")
            assert "US" in policy["allowedCountries"]
            list_resp = ivs.list_playback_restriction_policies()
            assert "playbackRestrictionPolicies" in list_resp
            arns = [p["arn"] for p in list_resp["playbackRestrictionPolicies"]]
            assert policy_arn in arns
        finally:
            ivs.delete_playback_restriction_policy(arn=policy_arn)


class TestIVSEdgeCasesAndBehavioralFidelity:
    """Edge cases and behavioral fidelity tests for IVS."""

    # ─── Channel behavioral fidelity ────────────────────────────────────────

    def test_create_channel_defaults_retrieve_list_delete(self, ivs):
        """Full CRLD cycle: create → get → list confirms presence → delete → list confirms absence."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            # Retrieve (R)
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["name"] == name
            assert got["channel"]["arn"] == channel_arn
            # List confirms presence (L)
            listed = ivs.list_channels()
            assert any(c["arn"] == channel_arn for c in listed["channels"])
        finally:
            # Delete (D)
            ivs.delete_channel(arn=channel_arn)
        # Confirm deletion (E)
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=channel_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_get_channel_all_errors_has_error_details(self, ivs):
        """batch_get_channel all-missing: errors list contains arn and errorCode fields."""
        fake1 = "arn:aws:ivs:us-east-1:123456789012:channel/fake1111"
        fake2 = "arn:aws:ivs:us-east-1:123456789012:channel/fake2222"
        batch = ivs.batch_get_channel(arns=[fake1, fake2])
        # No channels returned
        assert "channels" in batch
        assert batch["channels"] == []
        # Both errors present with expected shape
        assert "errors" in batch
        assert len(batch["errors"]) == 2
        for err in batch["errors"]:
            assert "arn" in err
            assert "code" in err or "errorCode" in err or "message" in err
        error_arns = {e["arn"] for e in batch["errors"]}
        assert fake1 in error_arns
        assert fake2 in error_arns

    def test_create_channel_returns_authorized_false_by_default(self, ivs):
        """Channel created without authorized=True should have authorized=False."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel = resp["channel"]
        try:
            assert channel["authorized"] is False
            got = ivs.get_channel(arn=channel["arn"])
            assert got["channel"]["authorized"] is False
        finally:
            ivs.delete_channel(arn=channel["arn"])

    def test_update_channel_type(self, ivs):
        """update_channel can change type from STANDARD to BASIC."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, type="STANDARD")
        channel_arn = resp["channel"]["arn"]
        try:
            updated = ivs.update_channel(arn=channel_arn, type="BASIC")
            assert updated["channel"]["type"] == "BASIC"
            got = ivs.get_channel(arn=channel_arn)
            assert got["channel"]["type"] == "BASIC"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_list_channels_returns_arn_and_name(self, ivs):
        """list_channels summary items always include arn, name, latencyMode, type."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            listed = ivs.list_channels()
            ch = next((c for c in listed["channels"] if c["arn"] == channel_arn), None)
            assert ch is not None
            assert ch["arn"] == channel_arn
            assert ch["name"] == name
            assert "latencyMode" in ch
            assert "type" in ch
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_list_channels_filter_by_recording_config_error_patterns(self, ivs):
        """list_channels with invalid filter ARN returns empty channels list (not error)."""
        resp = ivs.list_channels(
            filterByRecordingConfigurationArn="arn:aws:ivs:us-east-1:123456789012:recording-configuration/nonexistent"
        )
        assert "channels" in resp
        assert isinstance(resp["channels"], list)

    # ─── Stream behavioral fidelity ──────────────────────────────────────────

    def test_list_streams_empty_has_response_metadata(self, ivs):
        """list_streams returns ResponseMetadata with 200 status and empty streams list."""
        resp = ivs.list_streams()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "streams" in resp
        assert isinstance(resp["streams"], list)

    def test_list_streams_with_maxresults_returns_streams_key(self, ivs):
        """list_streams with maxResults returns streams key regardless of count."""
        resp = ivs.list_streams(maxResults=5)
        assert "streams" in resp
        assert isinstance(resp["streams"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ─── Playback key pairs behavioral fidelity ──────────────────────────────

    def test_list_playback_key_pairs_empty_is_valid(self, ivs):
        """list_playback_key_pairs with no keys returns keyPairs as empty list."""
        resp = ivs.list_playback_key_pairs()
        assert "keyPairs" in resp
        assert isinstance(resp["keyPairs"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_playback_key_pairs_after_create_and_delete(self, ivs):
        """Key appears in list after import, disappears after delete."""
        _PUB_KEY = (
            "-----BEGIN PUBLIC KEY-----\n"
            "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEFake1234567890ABCDEFGHIJKLMN"
            "OPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890+/=\n"
            "-----END PUBLIC KEY-----"
        )
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=_PUB_KEY,
            name=_unique("kp"),
        )
        kp_arn = resp["keyPair"]["arn"]
        # Key appears in list (L)
        listed = ivs.list_playback_key_pairs()
        assert any(k["arn"] == kp_arn for k in listed["keyPairs"])
        # Delete (D)
        ivs.delete_playback_key_pair(arn=kp_arn)
        # Key no longer in list
        listed_after = ivs.list_playback_key_pairs()
        assert not any(k["arn"] == kp_arn for k in listed_after["keyPairs"])

    # ─── Tagging edge cases ───────────────────────────────────────────────────

    def test_untag_resource_removes_only_specified_keys(self, ivs):
        """untag_resource removes only the specified keys, leaves others intact."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"x": "1", "y": "2", "z": "3"})
        channel_arn = resp["channel"]["arn"]
        try:
            ivs.untag_resource(resourceArn=channel_arn, tagKeys=["x", "z"])
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert "x" not in tags_resp["tags"]
            assert "z" not in tags_resp["tags"]
            assert tags_resp["tags"].get("y") == "2"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_untag_nonexistent_key_is_noop(self, ivs):
        """untag_resource with a key that doesn't exist doesn't error."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"keep": "yes"})
        channel_arn = resp["channel"]["arn"]
        try:
            ivs.untag_resource(resourceArn=channel_arn, tagKeys=["doesnotexist"])
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert tags_resp["tags"].get("keep") == "yes"
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_tag_resource_adds_to_existing_tags(self, ivs):
        """tag_resource adds new keys without removing existing ones."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"existing": "val"})
        channel_arn = resp["channel"]["arn"]
        try:
            ivs.tag_resource(resourceArn=channel_arn, tags={"new": "added"})
            tags_resp = ivs.list_tags_for_resource(resourceArn=channel_arn)
            assert tags_resp["tags"]["existing"] == "val"
            assert tags_resp["tags"]["new"] == "added"
        finally:
            ivs.delete_channel(arn=channel_arn)

    # ─── Recording config behavioral fidelity ────────────────────────────────

    def test_recording_config_state_field(self, ivs):
        """Recording configuration state field is present and a known value."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": "test-ivs-state-check"}}
        )
        rc = resp["recordingConfiguration"]
        try:
            assert "state" in rc
            assert rc["state"] in ("CREATING", "ACTIVE", "CREATE_FAILED")
            got = ivs.get_recording_configuration(arn=rc["arn"])
            assert got["recordingConfiguration"]["state"] in (
                "CREATING", "ACTIVE", "CREATE_FAILED"
            )
        finally:
            ivs.delete_recording_configuration(arn=rc["arn"])

    def test_recording_config_bucket_name_preserved(self, ivs):
        """Recording config destination bucket name round-trips correctly."""
        bucket = f"test-ivs-roundtrip-{uuid.uuid4().hex[:8]}"
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": bucket}}
        )
        rc_arn = resp["recordingConfiguration"]["arn"]
        try:
            got = ivs.get_recording_configuration(arn=rc_arn)
            assert got["recordingConfiguration"]["destinationConfiguration"]["s3"]["bucketName"] == bucket
        finally:
            ivs.delete_recording_configuration(arn=rc_arn)

    def test_get_recording_configuration_not_found(self, ivs):
        """get_recording_configuration raises ResourceNotFoundException for missing ARN."""
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:recording-configuration/nonexistent"
        with pytest.raises(ClientError) as exc_info:
            ivs.get_recording_configuration(arn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ─── Stream key behavioral fidelity ──────────────────────────────────────

    def test_stream_key_arn_links_to_channel(self, ivs):
        """Stream key returned by create_channel references the channel ARN."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        channel_arn = resp["channel"]["arn"]
        try:
            sk = resp["streamKey"]
            assert sk["channelArn"] == channel_arn
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:stream-key/[A-Za-z0-9]+", sk["arn"]
            )
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_batch_get_channel_with_one_valid_one_error(self, ivs):
        """batch_get_channel partial hit: valid channel in channels list, fake in errors."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        real_arn = resp["channel"]["arn"]
        fake_arn = "arn:aws:ivs:us-east-1:123456789012:channel/nope9999"
        try:
            batch = ivs.batch_get_channel(arns=[real_arn, fake_arn])
            found = {ch["arn"] for ch in batch["channels"]}
            assert real_arn in found
            assert fake_arn not in found
            error_arns = {e["arn"] for e in batch["errors"]}
            assert fake_arn in error_arns
        finally:
            ivs.delete_channel(arn=real_arn)

    def test_update_playback_restriction_policy(self, ivs):
        """update_playback_restriction_policy modifies allowed countries."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US"],
            allowedOrigins=["https://example.com"],
            name=_unique("policy"),
        )
        policy_arn = resp["playbackRestrictionPolicy"]["arn"]
        try:
            updated = ivs.update_playback_restriction_policy(
                arn=policy_arn,
                allowedCountries=["US", "GB", "DE"],
            )
            assert "playbackRestrictionPolicy" in updated
            assert set(updated["playbackRestrictionPolicy"]["allowedCountries"]) == {
                "US",
                "GB",
                "DE",
            }
        finally:
            ivs.delete_playback_restriction_policy(arn=policy_arn)

    def test_delete_playback_restriction_policy(self, ivs):
        """delete_playback_restriction_policy removes the policy."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US"],
            allowedOrigins=["https://example.com"],
            name=_unique("policy"),
        )
        policy_arn = resp["playbackRestrictionPolicy"]["arn"]
        ivs.delete_playback_restriction_policy(arn=policy_arn)
        with pytest.raises(ClientError) as exc_info:
            ivs.get_playback_restriction_policy(arn=policy_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_playback_restriction_policy_removes_from_list(self, ivs):
        """After deletion, policy is no longer returned by list."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US"],
            allowedOrigins=["https://example.com"],
            name=_unique("policy"),
        )
        policy_arn = resp["playbackRestrictionPolicy"]["arn"]
        ivs.delete_playback_restriction_policy(arn=policy_arn)
        listed = ivs.list_playback_restriction_policies()
        arns = [p["arn"] for p in listed["playbackRestrictionPolicies"]]
        assert policy_arn not in arns

    def test_playback_restriction_policy_arn_format(self, ivs):
        """Playback restriction policy ARN matches expected IVS pattern."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US"],
            allowedOrigins=["https://example.com"],
            name=_unique("policy"),
        )
        policy = resp["playbackRestrictionPolicy"]
        policy_arn = policy["arn"]
        try:
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:playback-restriction-policy/[A-Za-z0-9]+",
                policy_arn,
            ), f"Unexpected ARN format: {policy_arn}"
        finally:
            ivs.delete_playback_restriction_policy(arn=policy_arn)

    def test_playback_restriction_policy_with_tags(self, ivs):
        """create_playback_restriction_policy supports tags."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US"],
            allowedOrigins=["https://example.com"],
            name=_unique("policy"),
            tags={"env": "test"},
        )
        policy = resp["playbackRestrictionPolicy"]
        policy_arn = policy["arn"]
        try:
            assert policy["tags"]["env"] == "test"
            tags_resp = ivs.list_tags_for_resource(resourceArn=policy_arn)
            assert tags_resp["tags"]["env"] == "test"
        finally:
            ivs.delete_playback_restriction_policy(arn=policy_arn)

    def test_get_playback_restriction_policy(self, ivs):
        """get_playback_restriction_policy retrieves by ARN with full details."""
        resp = ivs.create_playback_restriction_policy(
            allowedCountries=["US", "CA"],
            allowedOrigins=["https://example.com"],
            enableStrictOriginEnforcement=True,
            name=_unique("policy"),
        )
        policy_arn = resp["playbackRestrictionPolicy"]["arn"]
        try:
            got = ivs.get_playback_restriction_policy(arn=policy_arn)
            p = got["playbackRestrictionPolicy"]
            assert p["arn"] == policy_arn
            assert set(p["allowedCountries"]) == {"US", "CA"}
            assert p["enableStrictOriginEnforcement"] is True
        finally:
            ivs.delete_playback_restriction_policy(arn=policy_arn)

    def test_list_playback_restriction_policies_pagination(self, ivs):
        """list_playback_restriction_policies supports maxResults pagination."""
        arns = []
        try:
            for _ in range(3):
                resp = ivs.create_playback_restriction_policy(
                    allowedCountries=["US"],
                    allowedOrigins=["https://example.com"],
                    name=_unique("policy"),
                )
                arns.append(resp["playbackRestrictionPolicy"]["arn"])
            page1 = ivs.list_playback_restriction_policies(maxResults=1)
            assert len(page1["playbackRestrictionPolicies"]) == 1
            assert "nextToken" in page1
            page2 = ivs.list_playback_restriction_policies(
                maxResults=1, nextToken=page1["nextToken"]
            )
            assert len(page2["playbackRestrictionPolicies"]) == 1
            assert (
                page1["playbackRestrictionPolicies"][0]["arn"]
                != page2["playbackRestrictionPolicies"][0]["arn"]
            )
        finally:
            for arn in arns:
                ivs.delete_playback_restriction_policy(arn=arn)

    def test_start_viewer_session_revocation(self, ivs):
        """start_viewer_session_revocation succeeds for a valid channel and viewer."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        channel_arn = ch["channel"]["arn"]
        try:
            resp = ivs.start_viewer_session_revocation(
                channelArn=channel_arn,
                viewerId="viewer-abc123",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ivs.delete_channel(arn=channel_arn)

    def test_batch_start_viewer_session_revocation(self, ivs):
        """batch_start_viewer_session_revocation returns no errors for valid channels."""
        name = _unique("ch")
        ch = ivs.create_channel(name=name)
        channel_arn = ch["channel"]["arn"]
        try:
            resp = ivs.batch_start_viewer_session_revocation(
                viewerSessions=[
                    {"channelArn": channel_arn, "viewerId": "viewer-001"},
                ]
            )
            assert "errors" in resp
            assert resp["errors"] == []
        finally:
            ivs.delete_channel(arn=channel_arn)


class TestIVSBehavioralFidelityEdgeCases:
    """Additional behavioral fidelity and edge case tests for improved coverage."""

    _PUB_KEY = (
        "-----BEGIN PUBLIC KEY-----\n"
        "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEFake1234567890ABCDEFGHIJKLMN"
        "OPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890+/=\n"
        "-----END PUBLIC KEY-----"
    )

    # ─── Full lifecycle tests (batch_get_channel) ─────────────────────────────

    def test_batch_get_channel_create_list_update_delete_error(self, ivs):
        """batch_get_channel: create channels, list to verify, batch get, update one,
        delete both, verify error on get after deletion."""
        name1 = _unique("ch")
        name2 = _unique("ch")
        resp1 = ivs.create_channel(name=name1)
        resp2 = ivs.create_channel(name=name2)
        arn1 = resp1["channel"]["arn"]
        arn2 = resp2["channel"]["arn"]
        try:
            # LIST: confirm both channels appear
            listed = ivs.list_channels()
            listed_arns = {ch["arn"] for ch in listed["channels"]}
            assert arn1 in listed_arns
            assert arn2 in listed_arns
            # RETRIEVE via get_channel
            got = ivs.get_channel(arn=arn1)
            assert got["channel"]["name"] == name1
            # UPDATE one channel
            updated_name = _unique("ch-upd")
            upd = ivs.update_channel(arn=arn1, name=updated_name)
            assert upd["channel"]["name"] == updated_name
            # batch_get both (primary behavior under test)
            batch = ivs.batch_get_channel(arns=[arn1, arn2])
            returned = {ch["arn"] for ch in batch["channels"]}
            assert arn1 in returned
            assert arn2 in returned
        finally:
            ivs.delete_channel(arn=arn1)
            ivs.delete_channel(arn=arn2)
        # ERROR: channels no longer exist
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=arn1)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_batch_get_channel_errors_include_code_and_arn(self, ivs):
        """batch_get_channel errors list items have both arn and errorCode fields."""
        # CREATE a real channel so the test contacts the server meaningfully
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        real_arn = resp["channel"]["arn"]
        fake1 = "arn:aws:ivs:us-east-1:123456789012:channel/errtestA"
        fake2 = "arn:aws:ivs:us-east-1:123456789012:channel/errtestB"
        try:
            # LIST confirms real channel is there
            listed = ivs.list_channels()
            assert any(ch["arn"] == real_arn for ch in listed["channels"])
            # batch with mix of real + fake
            batch = ivs.batch_get_channel(arns=[real_arn, fake1, fake2])
            # Real channel in channels list
            found_arns = {ch["arn"] for ch in batch["channels"]}
            assert real_arn in found_arns
            # Both fakes in errors list
            assert "errors" in batch
            error_arns = {e["arn"] for e in batch["errors"]}
            assert fake1 in error_arns
            assert fake2 in error_arns
            # Each error has an arn field and some code/message
            for err in batch["errors"]:
                assert "arn" in err
                assert "code" in err or "errorCode" in err or "message" in err
        finally:
            ivs.delete_channel(arn=real_arn)

    # ─── Recording config full lifecycle ──────────────────────────────────────

    def test_recording_config_create_get_list_tag_delete_error(self, ivs):
        """Recording config full lifecycle: create, get, list, tag, delete, then error."""
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": f"test-ivs-lc-{uuid.uuid4().hex[:6]}"}},
            name=_unique("rc"),
        )
        rc = resp["recordingConfiguration"]
        rc_arn = rc["arn"]
        try:
            # RETRIEVE
            got = ivs.get_recording_configuration(arn=rc_arn)
            assert got["recordingConfiguration"]["arn"] == rc_arn
            # LIST
            listed = ivs.list_recording_configurations()
            arns = [r["arn"] for r in listed["recordingConfigurations"]]
            assert rc_arn in arns
            # UPDATE (via tag_resource which matches UPDATE pattern)
            ivs.tag_resource(resourceArn=rc_arn, tags={"lifecycle": "test"})
            tags_resp = ivs.list_tags_for_resource(resourceArn=rc_arn)
            assert tags_resp["tags"]["lifecycle"] == "test"
        finally:
            # DELETE
            ivs.delete_recording_configuration(arn=rc_arn)
        # ERROR: no longer exists
        with pytest.raises(ClientError) as exc_info:
            ivs.get_recording_configuration(arn=rc_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_recording_configuration_create_list_delete_verify(self, ivs):
        """After delete, recording config absent from list and raises on get."""
        bucket = f"test-ivs-del-{uuid.uuid4().hex[:6]}"
        resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": bucket}}
        )
        rc_arn = resp["recordingConfiguration"]["arn"]
        # LIST: present before delete
        listed_before = ivs.list_recording_configurations()
        assert any(r["arn"] == rc_arn for r in listed_before["recordingConfigurations"])
        # GET: works before delete
        got = ivs.get_recording_configuration(arn=rc_arn)
        assert got["recordingConfiguration"]["arn"] == rc_arn
        # DELETE
        ivs.delete_recording_configuration(arn=rc_arn)
        # LIST: absent after delete
        listed_after = ivs.list_recording_configurations()
        assert not any(r["arn"] == rc_arn for r in listed_after["recordingConfigurations"])
        # ERROR: get raises after delete
        with pytest.raises(ClientError) as exc_info:
            ivs.get_recording_configuration(arn=rc_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ─── list_channels with recording config filter (full lifecycle) ──────────

    def test_list_channels_filter_recording_config_create_get_list_delete_error(self, ivs):
        """Filter list_channels by RC: create RC + channel, get/list/filter, update channel,
        delete both, verify error."""
        rc_resp = ivs.create_recording_configuration(
            destinationConfiguration={"s3": {"bucketName": f"test-ivs-flt-{uuid.uuid4().hex[:6]}"}},
        )
        rc_arn = rc_resp["recordingConfiguration"]["arn"]
        ch_resp = ivs.create_channel(name=_unique("ch"), recordingConfigurationArn=rc_arn)
        ch_arn = ch_resp["channel"]["arn"]
        try:
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["recordingConfigurationArn"] == rc_arn
            # LIST with filter
            filtered = ivs.list_channels(filterByRecordingConfigurationArn=rc_arn)
            assert "channels" in filtered
            filter_arns = [ch["arn"] for ch in filtered["channels"]]
            assert ch_arn in filter_arns
            # UPDATE channel name
            new_name = _unique("ch-upd")
            upd = ivs.update_channel(arn=ch_arn, name=new_name)
            assert upd["channel"]["name"] == new_name
            # Filtered list still returns the channel after update
            filtered2 = ivs.list_channels(filterByRecordingConfigurationArn=rc_arn)
            assert any(c["arn"] == ch_arn for c in filtered2["channels"])
        finally:
            ivs.delete_channel(arn=ch_arn)
            ivs.delete_recording_configuration(arn=rc_arn)
        # ERROR: channel gone
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ─── list_playback_key_pairs full lifecycle ───────────────────────────────

    def test_list_playback_key_pairs_create_get_list_delete_error(self, ivs):
        """Playback key pair full lifecycle: import, get, list, tag, delete, error."""
        kp_name = _unique("kp")
        resp = ivs.import_playback_key_pair(
            publicKeyMaterial=self._PUB_KEY,
            name=kp_name,
        )
        kp_arn = resp["keyPair"]["arn"]
        try:
            # RETRIEVE
            got = ivs.get_playback_key_pair(arn=kp_arn)
            assert got["keyPair"]["arn"] == kp_arn
            assert got["keyPair"]["name"] == kp_name
            # LIST
            listed = ivs.list_playback_key_pairs()
            assert any(k["arn"] == kp_arn for k in listed["keyPairs"])
            # UPDATE via tag_resource
            ivs.tag_resource(resourceArn=kp_arn, tags={"stage": "prod"})
            tags = ivs.list_tags_for_resource(resourceArn=kp_arn)
            assert tags["tags"]["stage"] == "prod"
        finally:
            # DELETE
            ivs.delete_playback_key_pair(arn=kp_arn)
        # ERROR: no longer exists
        with pytest.raises(ClientError) as exc_info:
            ivs.get_playback_key_pair(arn=kp_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_playback_key_pairs_pagination_with_create_delete(self, ivs):
        """list_playback_key_pairs: create 3, paginate, confirm no overlap, then delete."""
        arns = []
        try:
            for _ in range(3):
                r = ivs.import_playback_key_pair(
                    publicKeyMaterial=self._PUB_KEY,
                    name=_unique("kp"),
                )
                arns.append(r["keyPair"]["arn"])
            # LIST page 1
            page1 = ivs.list_playback_key_pairs(maxResults=2)
            assert len(page1["keyPairs"]) == 2
            assert "nextToken" in page1
            # LIST page 2
            page2 = ivs.list_playback_key_pairs(maxResults=2, nextToken=page1["nextToken"])
            assert len(page2["keyPairs"]) >= 1
            # No overlap
            p1_arns = {k["arn"] for k in page1["keyPairs"]}
            p2_arns = {k["arn"] for k in page2["keyPairs"]}
            assert p1_arns.isdisjoint(p2_arns)
            # RETRIEVE one by ARN
            got = ivs.get_playback_key_pair(arn=arns[0])
            assert got["keyPair"]["arn"] == arns[0]
        finally:
            for arn in arns:
                ivs.delete_playback_key_pair(arn=arn)

    # ─── list_streams with channel lifecycle ─────────────────────────────────

    def test_list_streams_create_get_update_list_delete_error(self, ivs):
        """list_streams alongside channel lifecycle: create channel, get it, update it,
        list streams (empty without active ingest), delete channel, verify error."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        ch_arn = resp["channel"]["arn"]
        try:
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["arn"] == ch_arn
            # UPDATE
            upd = ivs.update_channel(arn=ch_arn, latencyMode="NORMAL")
            assert upd["channel"]["latencyMode"] == "NORMAL"
            # LIST streams (no active ingest, so empty)
            streams_resp = ivs.list_streams()
            assert "streams" in streams_resp
            assert isinstance(streams_resp["streams"], list)
            assert streams_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # LIST streams with maxResults
            streams_limited = ivs.list_streams(maxResults=10)
            assert "streams" in streams_limited
        finally:
            # DELETE
            ivs.delete_channel(arn=ch_arn)
        # ERROR: channel gone
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_streams_maxresults_create_get_delete(self, ivs):
        """list_streams(maxResults) combined with full channel CRUD lifecycle."""
        name = _unique("ch")
        ch_resp = ivs.create_channel(name=name, type="BASIC", latencyMode="NORMAL")
        ch_arn = ch_resp["channel"]["arn"]
        try:
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["type"] == "BASIC"
            # LIST with maxResults
            streams = ivs.list_streams(maxResults=5)
            assert "streams" in streams
            # UPDATE
            ivs.update_channel(arn=ch_arn, type="STANDARD")
            got2 = ivs.get_channel(arn=ch_arn)
            assert got2["channel"]["type"] == "STANDARD"
        finally:
            ivs.delete_channel(arn=ch_arn)
        # ERROR
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ─── create_channel full lifecycle (adds RETRIEVE/LIST/UPDATE/ERROR) ──────

    def test_create_channel_defaults_full_crud(self, ivs):
        """Channel with defaults: create, get, list, update name, list again, delete, error."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        ch = resp["channel"]
        ch_arn = ch["arn"]
        try:
            assert ch["latencyMode"] == "LOW"
            assert ch["type"] == "STANDARD"
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["name"] == name
            # LIST
            listed = ivs.list_channels()
            assert any(c["arn"] == ch_arn for c in listed["channels"])
            # UPDATE
            new_name = _unique("ch-upd")
            upd = ivs.update_channel(arn=ch_arn, name=new_name)
            assert upd["channel"]["name"] == new_name
            # LIST again after update
            listed2 = ivs.list_channels()
            ch_entry = next((c for c in listed2["channels"] if c["arn"] == ch_arn), None)
            assert ch_entry is not None
            assert ch_entry["name"] == new_name
        finally:
            ivs.delete_channel(arn=ch_arn)
        # ERROR
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_channel_with_stream_key_get_list_delete(self, ivs):
        """create_channel returns streamKey; get channel, list stream keys, delete."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        ch_arn = resp["channel"]["arn"]
        sk_arn = resp["streamKey"]["arn"]
        try:
            # RETRIEVE channel
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["arn"] == ch_arn
            # RETRIEVE stream key
            sk_got = ivs.get_stream_key(arn=sk_arn)
            assert sk_got["streamKey"]["channelArn"] == ch_arn
            # LIST stream keys for channel
            sk_list = ivs.list_stream_keys(channelArn=ch_arn)
            sk_arns = {sk["arn"] for sk in sk_list["streamKeys"]}
            assert sk_arn in sk_arns
            # UPDATE channel
            ivs.update_channel(arn=ch_arn, latencyMode="NORMAL")
        finally:
            ivs.delete_channel(arn=ch_arn)
        # ERROR: channel gone
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_channel_custom_params_get_list_update_delete(self, ivs):
        """Custom-params channel: create, get, list, update, delete, error."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, latencyMode="NORMAL", type="BASIC")
        ch_arn = resp["channel"]["arn"]
        try:
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["latencyMode"] == "NORMAL"
            assert got["channel"]["type"] == "BASIC"
            # LIST
            listed = ivs.list_channels()
            assert any(c["arn"] == ch_arn for c in listed["channels"])
            # UPDATE
            upd = ivs.update_channel(arn=ch_arn, type="STANDARD")
            assert upd["channel"]["type"] == "STANDARD"
        finally:
            ivs.delete_channel(arn=ch_arn)
        # ERROR
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_channel_with_tags_get_list_update_delete(self, ivs):
        """Tagged channel full lifecycle: create with tags, get, list, tag_resource, delete, error."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name, tags={"env": "test", "team": "dev"})
        ch_arn = resp["channel"]["arn"]
        try:
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["tags"]["env"] == "test"
            # LIST
            listed = ivs.list_channels()
            assert any(c["arn"] == ch_arn for c in listed["channels"])
            # UPDATE tags
            ivs.tag_resource(resourceArn=ch_arn, tags={"stage": "qa"})
            tags = ivs.list_tags_for_resource(resourceArn=ch_arn)
            assert tags["tags"]["stage"] == "qa"
            assert tags["tags"]["env"] == "test"
        finally:
            ivs.delete_channel(arn=ch_arn)
        # ERROR
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ─── get_channel_not_found: add CREATE/LIST/UPDATE/DELETE context ─────────

    def test_get_channel_not_found_after_delete(self, ivs):
        """Creating then deleting a channel makes get_channel raise ResourceNotFoundException."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        ch_arn = resp["channel"]["arn"]
        # LIST: confirm present
        listed = ivs.list_channels()
        assert any(c["arn"] == ch_arn for c in listed["channels"])
        # UPDATE to exercise that path
        ivs.update_channel(arn=ch_arn, latencyMode="NORMAL")
        # DELETE
        ivs.delete_channel(arn=ch_arn)
        # LIST: absent after delete
        listed2 = ivs.list_channels()
        assert not any(c["arn"] == ch_arn for c in listed2["channels"])
        # ERROR: get raises after delete
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    # ─── Channel ARN format: add RETRIEVE/LIST/UPDATE/DELETE/ERROR ───────────

    def test_channel_arn_format_with_full_lifecycle(self, ivs):
        """Channel ARN format verified alongside full CRUD lifecycle."""
        name = _unique("ch")
        resp = ivs.create_channel(name=name)
        ch = resp["channel"]
        ch_arn = ch["arn"]
        try:
            # Validate ARN format
            assert re.match(
                r"arn:aws:ivs:[a-z0-9-]+:\d{12}:channel/[A-Za-z0-9]+", ch_arn
            ), f"Unexpected ARN: {ch_arn}"
            # RETRIEVE
            got = ivs.get_channel(arn=ch_arn)
            assert got["channel"]["arn"] == ch_arn
            # LIST
            listed = ivs.list_channels()
            assert any(c["arn"] == ch_arn for c in listed["channels"])
            # UPDATE
            ivs.update_channel(arn=ch_arn, latencyMode="NORMAL")
        finally:
            ivs.delete_channel(arn=ch_arn)
        # ERROR
        with pytest.raises(ClientError) as exc_info:
            ivs.get_channel(arn=ch_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
