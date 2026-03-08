"""MediaLive compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def medialive():
    return make_client("medialive")


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestMediaLiveListOperations:
    def test_list_channels(self, medialive):
        response = medialive.list_channels()
        assert "Channels" in response
        assert isinstance(response["Channels"], list)

    def test_list_inputs(self, medialive):
        response = medialive.list_inputs()
        assert "Inputs" in response
        assert isinstance(response["Inputs"], list)


class TestMediaLiveInputCRUD:
    def test_create_and_describe_input(self, medialive):
        name = _uid("input")
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = resp["Input"]["Id"]
        try:
            desc = medialive.describe_input(InputId=inp_id)
            assert desc["Name"] == name
            assert desc["Type"] == "URL_PULL"
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_list_inputs_after_create(self, medialive):
        name = _uid("input")
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = resp["Input"]["Id"]
        try:
            listed = medialive.list_inputs()
            ids = [i["Id"] for i in listed["Inputs"]]
            assert inp_id in ids
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_delete_input(self, medialive):
        name = _uid("input")
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = resp["Input"]["Id"]
        medialive.delete_input(InputId=inp_id)
        desc = medialive.describe_input(InputId=inp_id)
        assert desc["State"] == "DELETED"


class TestMediaLiveChannelCRUD:
    def _make_channel(self, medialive, name, inp_id):
        return medialive.create_channel(
            Name=name,
            InputAttachments=[{"InputId": inp_id}],
            Destinations=[{"Id": "dest1", "Settings": [{"Url": "s3://bucket/output"}]}],
            EncoderSettings={
                "AudioDescriptions": [],
                "OutputGroups": [
                    {
                        "OutputGroupSettings": {
                            "ArchiveGroupSettings": {"Destination": {"DestinationRefId": "dest1"}}
                        },
                        "Outputs": [
                            {
                                "OutputSettings": {
                                    "ArchiveOutputSettings": {
                                        "ContainerSettings": {"M2tsSettings": {}}
                                    }
                                }
                            }
                        ],
                    }
                ],
                "TimecodeConfig": {"Source": "EMBEDDED"},
                "VideoDescriptions": [{"Name": "video_1"}],
            },
            InputSpecification={
                "Codec": "AVC",
                "Resolution": "HD",
                "MaximumBitrate": "MAX_20_MBPS",
            },
            RoleArn="arn:aws:iam::123456789012:role/MediaLiveRole",
        )

    def test_create_and_describe_channel(self, medialive):
        name = _uid("ch")
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, name, inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                desc = medialive.describe_channel(ChannelId=ch_id)
                assert desc["Name"] == name
                assert "State" in desc
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_list_channels_after_create(self, medialive):
        name = _uid("ch")
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, name, inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                listed = medialive.list_channels()
                ids = [c["Id"] for c in listed["Channels"]]
                assert ch_id in ids
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_delete_channel(self, medialive):
        name = _uid("ch")
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, name, inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            medialive.delete_channel(ChannelId=ch_id)
            desc = medialive.describe_channel(ChannelId=ch_id)
            assert desc["State"] == "DELETED"
        finally:
            medialive.delete_input(InputId=inp_id)
