"""MediaLive compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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

    def test_update_input(self, medialive):
        name = _uid("input")
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = resp["Input"]["Id"]
        try:
            new_name = _uid("inp-upd")
            update_resp = medialive.update_input(
                InputId=inp_id,
                Name=new_name,
                Sources=[{"Url": "http://example.com/stream2"}],
            )
            assert "Input" in update_resp
            assert update_resp["Input"]["Name"] == new_name
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_create_input_with_tags(self, medialive):
        name = _uid("input")
        tags = {"env": "test", "project": "robotocore"}
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
            Tags=tags,
        )
        inp_id = resp["Input"]["Id"]
        try:
            assert resp["Input"]["Tags"] == tags
            desc = medialive.describe_input(InputId=inp_id)
            assert desc["Tags"] == tags
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_input_state_transitions(self, medialive):
        """Input starts as CREATING, resolves to DETACHED on describe."""
        name = _uid("input")
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = resp["Input"]["Id"]
        try:
            # First describe should resolve CREATING -> DETACHED
            desc = medialive.describe_input(InputId=inp_id)
            assert desc["State"] == "DETACHED"
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_create_input_arn_format(self, medialive):
        name = _uid("input")
        resp = medialive.create_input(
            Name=name,
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = resp["Input"]["Id"]
        try:
            assert "Arn" in resp["Input"]
            assert "medialive" in resp["Input"]["Arn"]
        finally:
            medialive.delete_input(InputId=inp_id)


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

    def test_update_channel(self, medialive):
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            name = _uid("ch")
            ch_resp = self._make_channel(medialive, name, inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                new_name = _uid("ch-upd")
                update_resp = medialive.update_channel(
                    ChannelId=ch_id,
                    Name=new_name,
                    Destinations=[{"Id": "dest1", "Settings": [{"Url": "s3://bucket/output2"}]}],
                    EncoderSettings={
                        "AudioDescriptions": [],
                        "OutputGroups": [
                            {
                                "OutputGroupSettings": {
                                    "ArchiveGroupSettings": {
                                        "Destination": {"DestinationRefId": "dest1"}
                                    }
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
                    InputAttachments=[{"InputId": inp_id}],
                    InputSpecification={
                        "Codec": "AVC",
                        "Resolution": "HD",
                        "MaximumBitrate": "MAX_20_MBPS",
                    },
                    RoleArn="arn:aws:iam::123456789012:role/MediaLiveRole",
                )
                assert "Channel" in update_resp
                assert update_resp["Channel"]["Name"] == new_name
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_start_channel(self, medialive):
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, _uid("ch"), inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                start_resp = medialive.start_channel(ChannelId=ch_id)
                assert start_resp["State"] == "STARTING"
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_stop_channel(self, medialive):
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, _uid("ch"), inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                # Start first, then stop
                medialive.start_channel(ChannelId=ch_id)
                stop_resp = medialive.stop_channel(ChannelId=ch_id)
                assert stop_resp["State"] == "STOPPING"
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_channel_state_transitions(self, medialive):
        """Verify that transient states resolve on describe."""
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, _uid("ch"), inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                # After create, state is CREATING; describe should resolve to IDLE
                desc = medialive.describe_channel(ChannelId=ch_id)
                assert desc["State"] == "IDLE"

                # Start -> STARTING, describe -> RUNNING
                medialive.start_channel(ChannelId=ch_id)
                desc = medialive.describe_channel(ChannelId=ch_id)
                assert desc["State"] == "RUNNING"

                # Stop -> STOPPING, describe -> IDLE
                medialive.stop_channel(ChannelId=ch_id)
                desc = medialive.describe_channel(ChannelId=ch_id)
                assert desc["State"] == "IDLE"
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_create_channel_with_tags(self, medialive):
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            tags = {"env": "test", "service": "medialive"}
            ch_resp = medialive.create_channel(
                Name=_uid("ch"),
                InputAttachments=[{"InputId": inp_id}],
                Destinations=[{"Id": "dest1", "Settings": [{"Url": "s3://bucket/output"}]}],
                EncoderSettings={
                    "AudioDescriptions": [],
                    "OutputGroups": [
                        {
                            "OutputGroupSettings": {
                                "ArchiveGroupSettings": {
                                    "Destination": {"DestinationRefId": "dest1"}
                                }
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
                Tags=tags,
            )
            ch_id = ch_resp["Channel"]["Id"]
            try:
                assert ch_resp["Channel"]["Tags"] == tags
                desc = medialive.describe_channel(ChannelId=ch_id)
                assert desc["Tags"] == tags
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_channel_arn_format(self, medialive):
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, _uid("ch"), inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                assert "Arn" in ch_resp["Channel"]
                assert "medialive" in ch_resp["Channel"]["Arn"]
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)

    def test_channel_pipelines_running_count(self, medialive):
        """Standard channel class should have pipelinesRunningCount=2."""
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            ch_resp = self._make_channel(medialive, _uid("ch"), inp_id)
            ch_id = ch_resp["Channel"]["Id"]
            try:
                desc = medialive.describe_channel(ChannelId=ch_id)
                assert desc["PipelinesRunningCount"] == 2
            finally:
                medialive.delete_channel(ChannelId=ch_id)
        finally:
            medialive.delete_input(InputId=inp_id)


class TestMediaLiveOfferingOperations:
    """Tests for offering and reservation operations."""

    def test_list_offerings(self, medialive):
        """ListOfferings returns Offerings list."""
        resp = medialive.list_offerings()
        assert "Offerings" in resp
        assert isinstance(resp["Offerings"], list)

    def test_list_reservations(self, medialive):
        """ListReservations returns Reservations list."""
        resp = medialive.list_reservations()
        assert "Reservations" in resp
        assert isinstance(resp["Reservations"], list)

    def test_describe_offering_not_found(self, medialive):
        """DescribeOffering for nonexistent ID raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            medialive.describe_offering(OfferingId="nonexistent-offering-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_reservation_not_found(self, medialive):
        """DescribeReservation for nonexistent ID raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            medialive.describe_reservation(ReservationId="nonexistent-reservation-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveScheduleOperations:
    """Tests for schedule operations."""

    def _make_channel_with_input(self, medialive):
        """Helper: create input + channel, return (channel_id, input_id)."""
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        ch_resp = medialive.create_channel(
            Name=_uid("ch"),
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
        return ch_resp["Channel"]["Id"], inp_id

    def test_describe_schedule_not_found(self, medialive):
        """DescribeSchedule for nonexistent channel raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            medialive.describe_schedule(ChannelId="nonexistent-channel-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_schedule_empty(self, medialive):
        """DescribeSchedule for channel with no schedule returns empty ScheduleActions."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.describe_schedule(ChannelId=ch_id)
            assert "ScheduleActions" in resp
            assert isinstance(resp["ScheduleActions"], list)
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_list_alerts_empty(self, medialive):
        """ListAlerts for a channel with no alerts returns empty list."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.list_alerts(ChannelId=ch_id)
            assert "Alerts" in resp
            assert isinstance(resp["Alerts"], list)
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)


class TestMediaLiveThumbnailOperations:
    """Tests for thumbnail operations."""

    def _make_channel_with_input(self, medialive):
        """Helper: create input + channel, return (channel_id, input_id)."""
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        ch_resp = medialive.create_channel(
            Name=_uid("ch"),
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
        return ch_resp["Channel"]["Id"], inp_id

    def test_describe_thumbnails(self, medialive):
        """DescribeThumbnails returns ThumbnailDetails list."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.describe_thumbnails(
                ChannelId=ch_id,
                PipelineId="0",
                ThumbnailType="CURRENT_ACTIVE",
            )
            assert "ThumbnailDetails" in resp
            assert isinstance(resp["ThumbnailDetails"], list)
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)
