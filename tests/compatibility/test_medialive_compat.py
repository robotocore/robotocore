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
            # Collect all pages
            all_ids = []
            paginator = medialive.get_paginator("list_inputs")
            for page in paginator.paginate():
                all_ids.extend(i["Id"] for i in page["Inputs"])
            assert inp_id in all_ids
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
                # Collect all pages
                all_ids = []
                paginator = medialive.get_paginator("list_channels")
                for page in paginator.paginate():
                    all_ids.extend(c["Id"] for c in page["Channels"])
                assert ch_id in all_ids
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


class TestMediaLiveBatchScheduleOperations:
    """Tests for BatchUpdateSchedule and DeleteSchedule."""

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

    def test_batch_update_schedule_empty(self, medialive):
        """BatchUpdateSchedule with empty Creates and Deletes succeeds."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.batch_update_schedule(
                ChannelId=ch_id,
                Creates={"ScheduleActions": []},
            )
            assert "Creates" in resp
            assert "ScheduleActions" in resp["Creates"]
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_batch_update_schedule_add_action(self, medialive):
        """BatchUpdateSchedule can add a pause action."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.batch_update_schedule(
                ChannelId=ch_id,
                Creates={
                    "ScheduleActions": [
                        {
                            "ActionName": "pause-action",
                            "ScheduleActionStartSettings": {
                                "ImmediateModeScheduleActionStartSettings": {}
                            },
                            "ScheduleActionSettings": {
                                "PauseStateSettings": {
                                    "Pipelines": [
                                        {"PipelineId": "PIPELINE_0"},
                                    ]
                                }
                            },
                        }
                    ]
                },
            )
            assert "Creates" in resp
            created = resp["Creates"]["ScheduleActions"]
            assert len(created) >= 1
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_delete_schedule(self, medialive):
        """DeleteSchedule clears the schedule for a channel."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.delete_schedule(ChannelId=ch_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)


class TestMediaLivePartnerInput:
    """Tests for CreatePartnerInput."""

    def test_create_partner_input(self, medialive):
        """CreatePartnerInput creates a partner input from an existing input."""
        inp_resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp_id = inp_resp["Input"]["Id"]
        try:
            resp = medialive.create_partner_input(
                InputId=inp_id,
                RequestId=_uid("req"),
            )
            assert "Input" in resp
            partner_id = resp["Input"]["Id"]
            assert partner_id != inp_id
            # Cleanup partner input
            medialive.delete_input(InputId=partner_id)
        finally:
            medialive.delete_input(InputId=inp_id)


class TestMediaLiveReservationOperations:
    """Tests for PurchaseOffering, DeleteReservation, UpdateReservation."""

    def test_delete_reservation_not_found(self, medialive):
        """DeleteReservation for nonexistent ID raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            medialive.delete_reservation(ReservationId="nonexistent-reservation-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_update_reservation_not_found(self, medialive):
        """UpdateReservation for nonexistent ID raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            medialive.update_reservation(
                ReservationId="nonexistent-reservation-id",
                Name="new-name",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_purchase_offering_not_found(self, medialive):
        """PurchaseOffering with nonexistent offering raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            medialive.purchase_offering(
                OfferingId="nonexistent-offering-id",
                Count=1,
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveChannelAdvanced:
    """Tests for RestartChannelPipelines and UpdateChannelClass."""

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

    def test_update_channel_class(self, medialive):
        """UpdateChannelClass changes the channel class."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.update_channel_class(
                ChannelId=ch_id,
                ChannelClass="SINGLE_PIPELINE",
                Destinations=[{"Id": "dest1", "Settings": [{"Url": "s3://bucket/output"}]}],
            )
            assert "Channel" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_restart_channel_pipelines(self, medialive):
        """RestartChannelPipelines returns channel state."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.restart_channel_pipelines(
                ChannelId=ch_id,
                PipelineIds=["PIPELINE_0"],
            )
            assert "State" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_batch_start(self, medialive):
        """BatchStart with a channel ID."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.batch_start(ChannelIds=[ch_id])
            assert "Successful" in resp
            assert "Failed" in resp
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_batch_stop(self, medialive):
        """BatchStop with a channel ID."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            medialive.start_channel(ChannelId=ch_id)
            resp = medialive.batch_stop(ChannelIds=[ch_id])
            assert "Successful" in resp
            assert "Failed" in resp
        finally:
            medialive.delete_channel(ChannelId=ch_id)
            medialive.delete_input(InputId=inp_id)

    def test_batch_delete(self, medialive):
        """BatchDelete with a channel ID."""
        ch_id, inp_id = self._make_channel_with_input(medialive)
        try:
            resp = medialive.batch_delete(ChannelIds=[ch_id])
            assert "Successful" in resp
            assert "Failed" in resp
        finally:
            try:
                medialive.delete_channel(ChannelId=ch_id)
            except Exception:
                pass  # best-effort cleanup
            medialive.delete_input(InputId=inp_id)


class TestMediaLiveInputSecurityGroups:
    """Tests for Input Security Group CRUD."""

    def test_create_and_describe_input_security_group(self, medialive):
        resp = medialive.create_input_security_group(
            WhitelistRules=[{"Cidr": "10.0.0.0/8"}],
            Tags={"env": "test"},
        )
        sg_id = resp["SecurityGroup"]["Id"]
        try:
            desc = medialive.describe_input_security_group(InputSecurityGroupId=sg_id)
            assert desc["Id"] == sg_id
            assert "WhitelistRules" in desc
        finally:
            medialive.delete_input_security_group(InputSecurityGroupId=sg_id)

    def test_list_input_security_groups(self, medialive):
        resp = medialive.list_input_security_groups()
        assert "InputSecurityGroups" in resp
        assert isinstance(resp["InputSecurityGroups"], list)

    def test_list_input_security_groups_after_create(self, medialive):
        resp = medialive.create_input_security_group(
            WhitelistRules=[{"Cidr": "10.0.0.0/8"}],
        )
        sg_id = resp["SecurityGroup"]["Id"]
        try:
            all_ids = []
            paginator = medialive.get_paginator("list_input_security_groups")
            for page in paginator.paginate():
                all_ids.extend(sg["Id"] for sg in page["InputSecurityGroups"])
            assert sg_id in all_ids
        finally:
            medialive.delete_input_security_group(InputSecurityGroupId=sg_id)

    def test_update_input_security_group(self, medialive):
        resp = medialive.create_input_security_group(
            WhitelistRules=[{"Cidr": "10.0.0.0/8"}],
        )
        sg_id = resp["SecurityGroup"]["Id"]
        try:
            update_resp = medialive.update_input_security_group(
                InputSecurityGroupId=sg_id,
                WhitelistRules=[{"Cidr": "192.168.0.0/16"}],
            )
            assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            medialive.delete_input_security_group(InputSecurityGroupId=sg_id)

    def test_delete_input_security_group(self, medialive):
        resp = medialive.create_input_security_group(
            WhitelistRules=[{"Cidr": "10.0.0.0/8"}],
        )
        sg_id = resp["SecurityGroup"]["Id"]
        del_resp = medialive.delete_input_security_group(InputSecurityGroupId=sg_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_input_security_group_tags(self, medialive):
        tags = {"env": "test", "project": "robotocore"}
        resp = medialive.create_input_security_group(
            WhitelistRules=[{"Cidr": "10.0.0.0/8"}],
            Tags=tags,
        )
        sg_id = resp["SecurityGroup"]["Id"]
        try:
            assert resp["SecurityGroup"]["Tags"] == tags
        finally:
            medialive.delete_input_security_group(InputSecurityGroupId=sg_id)


class TestMediaLiveTagOperations:
    """Tests for ListTagsForResource, CreateTags, DeleteTags."""

    def test_list_tags_for_resource(self, medialive):
        resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
            Tags={"k1": "v1"},
        )
        inp = resp["Input"]
        try:
            tag_resp = medialive.list_tags_for_resource(ResourceArn=inp["Arn"])
            assert "Tags" in tag_resp
            assert isinstance(tag_resp["Tags"], dict)
        finally:
            medialive.delete_input(InputId=inp["Id"])

    def test_create_tags(self, medialive):
        resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
        )
        inp = resp["Input"]
        try:
            medialive.create_tags(
                ResourceArn=inp["Arn"],
                Tags={"newkey": "newval"},
            )
            tag_resp = medialive.list_tags_for_resource(ResourceArn=inp["Arn"])
            assert tag_resp["Tags"]["newkey"] == "newval"
        finally:
            medialive.delete_input(InputId=inp["Id"])

    def test_delete_tags(self, medialive):
        resp = medialive.create_input(
            Name=_uid("inp"),
            Type="URL_PULL",
            Sources=[{"Url": "http://example.com/stream"}],
            Tags={"k1": "v1", "k2": "v2"},
        )
        inp = resp["Input"]
        try:
            del_resp = medialive.delete_tags(ResourceArn=inp["Arn"], TagKeys=["k1"])
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            medialive.delete_input(InputId=inp["Id"])


class TestMediaLiveMultiplexCRUD:
    """Tests for Multiplex CRUD operations."""

    def _make_multiplex(self, medialive, name=None):
        name = name or _uid("mux")
        return medialive.create_multiplex(
            Name=name,
            AvailabilityZones=["us-east-1a", "us-east-1b"],
            MultiplexSettings={
                "TransportStreamBitrate": 1000000,
                "TransportStreamId": 1,
                "TransportStreamReservedBitrate": 100000,
                "MaximumVideoBufferDelayMilliseconds": 1000,
            },
            RequestId=_uid("req"),
        )

    def test_create_and_describe_multiplex(self, medialive):
        name = _uid("mux")
        resp = self._make_multiplex(medialive, name)
        mux_id = resp["Multiplex"]["Id"]
        try:
            desc = medialive.describe_multiplex(MultiplexId=mux_id)
            assert desc["Name"] == name
            assert desc["Id"] == mux_id
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)

    def test_list_multiplexes(self, medialive):
        resp = medialive.list_multiplexes()
        assert "Multiplexes" in resp
        assert isinstance(resp["Multiplexes"], list)

    def test_list_multiplexes_after_create(self, medialive):
        resp = self._make_multiplex(medialive)
        mux_id = resp["Multiplex"]["Id"]
        try:
            all_ids = []
            paginator = medialive.get_paginator("list_multiplexes")
            for page in paginator.paginate():
                all_ids.extend(m["Id"] for m in page["Multiplexes"])
            assert mux_id in all_ids
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)

    def test_update_multiplex(self, medialive):
        resp = self._make_multiplex(medialive)
        mux_id = resp["Multiplex"]["Id"]
        try:
            new_name = _uid("mux-upd")
            upd = medialive.update_multiplex(
                MultiplexId=mux_id,
                Name=new_name,
                MultiplexSettings={
                    "TransportStreamBitrate": 2000000,
                    "TransportStreamId": 1,
                    "TransportStreamReservedBitrate": 200000,
                    "MaximumVideoBufferDelayMilliseconds": 1000,
                },
            )
            assert "Multiplex" in upd
            assert upd["Multiplex"]["Name"] == new_name
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)

    def test_delete_multiplex(self, medialive):
        resp = self._make_multiplex(medialive)
        mux_id = resp["Multiplex"]["Id"]
        del_resp = medialive.delete_multiplex(MultiplexId=mux_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "State" in del_resp

    def test_start_multiplex(self, medialive):
        resp = self._make_multiplex(medialive)
        mux_id = resp["Multiplex"]["Id"]
        try:
            start = medialive.start_multiplex(MultiplexId=mux_id)
            assert "State" in start
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)

    def test_stop_multiplex(self, medialive):
        resp = self._make_multiplex(medialive)
        mux_id = resp["Multiplex"]["Id"]
        try:
            medialive.start_multiplex(MultiplexId=mux_id)
            stop = medialive.stop_multiplex(MultiplexId=mux_id)
            assert "State" in stop
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)


class TestMediaLiveMultiplexPrograms:
    """Tests for Multiplex Program CRUD."""

    def _make_multiplex(self, medialive):
        resp = medialive.create_multiplex(
            Name=_uid("mux"),
            AvailabilityZones=["us-east-1a", "us-east-1b"],
            MultiplexSettings={
                "TransportStreamBitrate": 1000000,
                "TransportStreamId": 1,
                "TransportStreamReservedBitrate": 100000,
                "MaximumVideoBufferDelayMilliseconds": 1000,
            },
            RequestId=_uid("req"),
        )
        return resp["Multiplex"]["Id"]

    def test_create_and_describe_multiplex_program(self, medialive):
        mux_id = self._make_multiplex(medialive)
        try:
            prog_name = _uid("prog")
            resp = medialive.create_multiplex_program(
                MultiplexId=mux_id,
                RequestId=_uid("req"),
                MultiplexProgramSettings={
                    "ProgramNumber": 1,
                    "PreferredChannelPipeline": "CURRENTLY_ACTIVE",
                    "VideoSettings": {
                        "ConstantBitrate": 500000,
                    },
                },
                ProgramName=prog_name,
            )
            assert "MultiplexProgram" in resp
            desc = medialive.describe_multiplex_program(
                MultiplexId=mux_id,
                ProgramName=prog_name,
            )
            assert desc["ProgramName"] == prog_name
            medialive.delete_multiplex_program(MultiplexId=mux_id, ProgramName=prog_name)
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)

    def test_list_multiplex_programs(self, medialive):
        mux_id = self._make_multiplex(medialive)
        try:
            resp = medialive.list_multiplex_programs(MultiplexId=mux_id)
            assert "MultiplexPrograms" in resp
            assert isinstance(resp["MultiplexPrograms"], list)
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)

    def test_update_multiplex_program(self, medialive):
        mux_id = self._make_multiplex(medialive)
        try:
            prog_name = _uid("prog")
            medialive.create_multiplex_program(
                MultiplexId=mux_id,
                RequestId=_uid("req"),
                MultiplexProgramSettings={
                    "ProgramNumber": 1,
                    "PreferredChannelPipeline": "CURRENTLY_ACTIVE",
                    "VideoSettings": {
                        "ConstantBitrate": 500000,
                    },
                },
                ProgramName=prog_name,
            )
            upd = medialive.update_multiplex_program(
                MultiplexId=mux_id,
                ProgramName=prog_name,
                MultiplexProgramSettings={
                    "ProgramNumber": 1,
                    "PreferredChannelPipeline": "PIPELINE_0",
                    "VideoSettings": {
                        "ConstantBitrate": 600000,
                    },
                },
            )
            assert "MultiplexProgram" in upd
            medialive.delete_multiplex_program(MultiplexId=mux_id, ProgramName=prog_name)
        finally:
            medialive.delete_multiplex(MultiplexId=mux_id)


class TestMediaLiveAccountConfiguration:
    """Tests for account configuration."""

    def test_describe_account_configuration(self, medialive):
        resp = medialive.describe_account_configuration()
        assert "AccountConfiguration" in resp

    def test_update_account_configuration(self, medialive):
        resp = medialive.update_account_configuration(
            AccountConfiguration={"KmsKeyId": "alias/test-key"},
        )
        assert "AccountConfiguration" in resp


class TestMediaLiveInputDevices:
    """Tests for input device operations."""

    def test_list_input_devices(self, medialive):
        resp = medialive.list_input_devices()
        assert "InputDevices" in resp
        assert isinstance(resp["InputDevices"], list)

    def test_list_input_device_transfers(self, medialive):
        resp = medialive.list_input_device_transfers(TransferType="INCOMING")
        assert "InputDeviceTransfers" in resp
        assert isinstance(resp["InputDeviceTransfers"], list)


class TestMediaLiveSignalMaps:
    """Tests for Signal Map CRUD."""

    def test_create_and_get_signal_map(self, medialive):
        name = _uid("sigmap")
        resp = medialive.create_signal_map(
            DiscoveryEntryPointArn="arn:aws:medialive:us-east-1:123456789012:channel:1234",
            Name=name,
        )
        sig_id = resp["Id"]
        try:
            desc = medialive.get_signal_map(Identifier=sig_id)
            assert desc["Name"] == name
            assert desc["Id"] == sig_id
        finally:
            medialive.delete_signal_map(Identifier=sig_id)

    def test_list_signal_maps(self, medialive):
        resp = medialive.list_signal_maps()
        assert "SignalMaps" in resp
        assert isinstance(resp["SignalMaps"], list)

    def test_start_update_signal_map(self, medialive):
        name = _uid("sigmap")
        resp = medialive.create_signal_map(
            DiscoveryEntryPointArn="arn:aws:medialive:us-east-1:123456789012:channel:1234",
            Name=name,
        )
        sig_id = resp["Id"]
        try:
            new_name = _uid("sigmap-upd")
            upd = medialive.start_update_signal_map(
                Identifier=sig_id,
                Name=new_name,
            )
            assert upd["Name"] == new_name
        finally:
            medialive.delete_signal_map(Identifier=sig_id)

    def test_delete_signal_map(self, medialive):
        resp = medialive.create_signal_map(
            DiscoveryEntryPointArn="arn:aws:medialive:us-east-1:123456789012:channel:1234",
            Name=_uid("sigmap"),
        )
        sig_id = resp["Id"]
        del_resp = medialive.delete_signal_map(Identifier=sig_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMediaLiveCloudWatchAlarmTemplates:
    """Tests for CloudWatch Alarm Template CRUD."""

    def _make_group(self, medialive, name=None):
        name = name or _uid("cw-grp")
        return medialive.create_cloud_watch_alarm_template_group(Name=name)

    def test_create_and_get_alarm_template_group(self, medialive):
        name = _uid("cw-grp")
        resp = self._make_group(medialive, name)
        grp_id = resp["Id"]
        try:
            desc = medialive.get_cloud_watch_alarm_template_group(Identifier=grp_id)
            assert desc["Name"] == name
        finally:
            medialive.delete_cloud_watch_alarm_template_group(Identifier=grp_id)

    def test_list_alarm_template_groups(self, medialive):
        resp = medialive.list_cloud_watch_alarm_template_groups()
        assert "CloudWatchAlarmTemplateGroups" in resp

    def test_delete_alarm_template_group(self, medialive):
        resp = self._make_group(medialive)
        grp_id = resp["Id"]
        del_resp = medialive.delete_cloud_watch_alarm_template_group(Identifier=grp_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_and_get_alarm_template(self, medialive):
        grp_resp = self._make_group(medialive)
        grp_id = grp_resp["Id"]
        try:
            name = _uid("cw-tmpl")
            resp = medialive.create_cloud_watch_alarm_template(
                Name=name,
                GroupIdentifier=grp_id,
                ComparisonOperator="GreaterThanOrEqualToThreshold",
                MetricName="NetworkIn",
                Statistic="Average",
                TargetResourceType="MEDIALIVE_CHANNEL",
                TreatMissingData="notBreaching",
                Period=300,
                EvaluationPeriods=1,
                Threshold=1000.0,
            )
            tmpl_id = resp["Id"]
            try:
                desc = medialive.get_cloud_watch_alarm_template(Identifier=tmpl_id)
                assert desc["Name"] == name
                assert desc["MetricName"] == "NetworkIn"
            finally:
                medialive.delete_cloud_watch_alarm_template(Identifier=tmpl_id)
        finally:
            medialive.delete_cloud_watch_alarm_template_group(Identifier=grp_id)

    def test_list_alarm_templates(self, medialive):
        resp = medialive.list_cloud_watch_alarm_templates()
        assert "CloudWatchAlarmTemplates" in resp

    def test_update_alarm_template(self, medialive):
        grp_resp = self._make_group(medialive)
        grp_id = grp_resp["Id"]
        try:
            resp = medialive.create_cloud_watch_alarm_template(
                Name=_uid("cw-tmpl"),
                GroupIdentifier=grp_id,
                ComparisonOperator="GreaterThanOrEqualToThreshold",
                MetricName="NetworkIn",
                Statistic="Average",
                TargetResourceType="MEDIALIVE_CHANNEL",
                TreatMissingData="notBreaching",
                Period=300,
                EvaluationPeriods=1,
                Threshold=1000.0,
            )
            tmpl_id = resp["Id"]
            try:
                upd = medialive.update_cloud_watch_alarm_template(
                    Identifier=tmpl_id,
                    Threshold=2000.0,
                )
                assert upd["Threshold"] == 2000.0
            finally:
                medialive.delete_cloud_watch_alarm_template(Identifier=tmpl_id)
        finally:
            medialive.delete_cloud_watch_alarm_template_group(Identifier=grp_id)


class TestMediaLiveEventBridgeRuleTemplates:
    """Tests for EventBridge Rule Template CRUD."""

    def _make_group(self, medialive, name=None):
        name = name or _uid("eb-grp")
        return medialive.create_event_bridge_rule_template_group(Name=name)

    def test_create_and_get_rule_template_group(self, medialive):
        name = _uid("eb-grp")
        resp = self._make_group(medialive, name)
        grp_id = resp["Id"]
        try:
            desc = medialive.get_event_bridge_rule_template_group(Identifier=grp_id)
            assert desc["Name"] == name
        finally:
            medialive.delete_event_bridge_rule_template_group(Identifier=grp_id)

    def test_list_rule_template_groups(self, medialive):
        resp = medialive.list_event_bridge_rule_template_groups()
        assert "EventBridgeRuleTemplateGroups" in resp

    def test_delete_rule_template_group(self, medialive):
        resp = self._make_group(medialive)
        grp_id = resp["Id"]
        del_resp = medialive.delete_event_bridge_rule_template_group(Identifier=grp_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_and_get_rule_template(self, medialive):
        grp_resp = self._make_group(medialive)
        grp_id = grp_resp["Id"]
        try:
            name = _uid("eb-tmpl")
            resp = medialive.create_event_bridge_rule_template(
                Name=name,
                GroupIdentifier=grp_id,
                EventType="MEDIALIVE_CHANNEL_ALERT",
            )
            tmpl_id = resp["Id"]
            try:
                desc = medialive.get_event_bridge_rule_template(Identifier=tmpl_id)
                assert desc["Name"] == name
                assert desc["EventType"] == "MEDIALIVE_CHANNEL_ALERT"
            finally:
                medialive.delete_event_bridge_rule_template(Identifier=tmpl_id)
        finally:
            medialive.delete_event_bridge_rule_template_group(Identifier=grp_id)

    def test_list_rule_templates(self, medialive):
        resp = medialive.list_event_bridge_rule_templates()
        assert "EventBridgeRuleTemplates" in resp

    def test_update_rule_template(self, medialive):
        grp_resp = self._make_group(medialive)
        grp_id = grp_resp["Id"]
        try:
            resp = medialive.create_event_bridge_rule_template(
                Name=_uid("eb-tmpl"),
                GroupIdentifier=grp_id,
                EventType="MEDIALIVE_CHANNEL_ALERT",
            )
            tmpl_id = resp["Id"]
            try:
                upd = medialive.update_event_bridge_rule_template(
                    Identifier=tmpl_id,
                    Description="Updated description",
                )
                assert upd["Description"] == "Updated description"
            finally:
                medialive.delete_event_bridge_rule_template(Identifier=tmpl_id)
        finally:
            medialive.delete_event_bridge_rule_template_group(Identifier=grp_id)


class TestMediaLiveClusterOperations:
    """Tests for Cluster CRUD."""

    def _make_network(self, medialive):
        resp = medialive.create_network(
            Name=_uid("net"),
            IpPools=[{"Cidr": "10.0.0.0/24"}],
            RequestId=_uid("req"),
        )
        return resp["Id"]

    def _make_cluster(self, medialive, network_id, name=None):
        name = name or _uid("cluster")
        return medialive.create_cluster(
            Name=name,
            ClusterType="ON_PREMISES",
            InstanceRoleArn="arn:aws:iam::123456789012:role/MediaLiveClusterRole",
            NetworkSettings={"DefaultRoute": "", "InterfaceMappings": []},
            RequestId=_uid("req"),
        )

    def test_create_and_describe_cluster(self, medialive):
        net_id = self._make_network(medialive)
        try:
            name = _uid("cluster")
            resp = self._make_cluster(medialive, net_id, name)
            cluster_id = resp["Id"]
            try:
                desc = medialive.describe_cluster(ClusterId=cluster_id)
                assert desc["Name"] == name
                assert desc["Id"] == cluster_id
            finally:
                medialive.delete_cluster(ClusterId=cluster_id)
        finally:
            medialive.delete_network(NetworkId=net_id)

    def test_list_clusters(self, medialive):
        resp = medialive.list_clusters()
        assert "Clusters" in resp
        assert isinstance(resp["Clusters"], list)

    def test_update_cluster(self, medialive):
        net_id = self._make_network(medialive)
        try:
            resp = self._make_cluster(medialive, net_id)
            cluster_id = resp["Id"]
            try:
                new_name = _uid("cl-upd")
                upd = medialive.update_cluster(
                    ClusterId=cluster_id,
                    Name=new_name,
                )
                assert upd["Name"] == new_name
            finally:
                medialive.delete_cluster(ClusterId=cluster_id)
        finally:
            medialive.delete_network(NetworkId=net_id)

    def test_delete_cluster(self, medialive):
        net_id = self._make_network(medialive)
        try:
            resp = self._make_cluster(medialive, net_id)
            cluster_id = resp["Id"]
            del_resp = medialive.delete_cluster(ClusterId=cluster_id)
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            medialive.delete_network(NetworkId=net_id)


class TestMediaLiveNodeOperations:
    """Tests for Node CRUD."""

    def _make_network(self, medialive):
        resp = medialive.create_network(
            Name=_uid("net"),
            IpPools=[{"Cidr": "10.0.0.0/24"}],
            RequestId=_uid("req"),
        )
        return resp["Id"]

    def _make_cluster(self, medialive, network_id):
        resp = medialive.create_cluster(
            Name=_uid("cluster"),
            ClusterType="ON_PREMISES",
            InstanceRoleArn="arn:aws:iam::123456789012:role/MediaLiveClusterRole",
            NetworkSettings={"DefaultRoute": "", "InterfaceMappings": []},
            RequestId=_uid("req"),
        )
        return resp["Id"]

    def test_create_and_describe_node(self, medialive):
        net_id = self._make_network(medialive)
        try:
            cluster_id = self._make_cluster(medialive, net_id)
            try:
                name = _uid("node")
                resp = medialive.create_node(
                    ClusterId=cluster_id,
                    Name=name,
                    NodeInterfaceMappings=[],
                    Role="BACKUP",
                    RequestId=_uid("req"),
                )
                node_id = resp["Id"]
                try:
                    desc = medialive.describe_node(ClusterId=cluster_id, NodeId=node_id)
                    assert desc["Name"] == name
                    assert desc["Id"] == node_id
                finally:
                    medialive.delete_node(ClusterId=cluster_id, NodeId=node_id)
            finally:
                medialive.delete_cluster(ClusterId=cluster_id)
        finally:
            medialive.delete_network(NetworkId=net_id)

    def test_list_nodes(self, medialive):
        net_id = self._make_network(medialive)
        try:
            cluster_id = self._make_cluster(medialive, net_id)
            try:
                resp = medialive.list_nodes(ClusterId=cluster_id)
                assert "Nodes" in resp
                assert isinstance(resp["Nodes"], list)
            finally:
                medialive.delete_cluster(ClusterId=cluster_id)
        finally:
            medialive.delete_network(NetworkId=net_id)

    def test_update_node(self, medialive):
        net_id = self._make_network(medialive)
        try:
            cluster_id = self._make_cluster(medialive, net_id)
            try:
                resp = medialive.create_node(
                    ClusterId=cluster_id,
                    Name=_uid("node"),
                    NodeInterfaceMappings=[],
                    Role="BACKUP",
                    RequestId=_uid("req"),
                )
                node_id = resp["Id"]
                try:
                    new_name = _uid("node-upd")
                    upd = medialive.update_node(
                        ClusterId=cluster_id,
                        NodeId=node_id,
                        Name=new_name,
                    )
                    assert upd["Name"] == new_name
                finally:
                    medialive.delete_node(ClusterId=cluster_id, NodeId=node_id)
            finally:
                medialive.delete_cluster(ClusterId=cluster_id)
        finally:
            medialive.delete_network(NetworkId=net_id)


class TestMediaLiveNetworkOperations:
    """Tests for Network CRUD."""

    def test_create_and_describe_network(self, medialive):
        name = _uid("net")
        resp = medialive.create_network(
            Name=name,
            IpPools=[{"Cidr": "10.0.0.0/24"}],
            RequestId=_uid("req"),
        )
        net_id = resp["Id"]
        try:
            desc = medialive.describe_network(NetworkId=net_id)
            assert desc["Name"] == name
            assert desc["Id"] == net_id
        finally:
            medialive.delete_network(NetworkId=net_id)

    def test_list_networks(self, medialive):
        resp = medialive.list_networks()
        assert "Networks" in resp
        assert isinstance(resp["Networks"], list)

    def test_update_network(self, medialive):
        resp = medialive.create_network(
            Name=_uid("net"),
            IpPools=[{"Cidr": "10.0.0.0/24"}],
            RequestId=_uid("req"),
        )
        net_id = resp["Id"]
        try:
            new_name = _uid("net-upd")
            upd = medialive.update_network(
                NetworkId=net_id,
                Name=new_name,
            )
            assert upd["Name"] == new_name
        finally:
            medialive.delete_network(NetworkId=net_id)

    def test_delete_network(self, medialive):
        resp = medialive.create_network(
            Name=_uid("net"),
            IpPools=[{"Cidr": "10.0.0.0/24"}],
            RequestId=_uid("req"),
        )
        net_id = resp["Id"]
        del_resp = medialive.delete_network(NetworkId=net_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_networks_after_create(self, medialive):
        resp = medialive.create_network(
            Name=_uid("net"),
            IpPools=[{"Cidr": "10.0.0.0/24"}],
            RequestId=_uid("req"),
        )
        net_id = resp["Id"]
        try:
            # Collect all pages to handle pagination
            all_ids = []
            paginator = medialive.get_paginator("list_networks")
            for page in paginator.paginate():
                all_ids.extend(n["Id"] for n in page["Networks"])
            assert net_id in all_ids
        finally:
            medialive.delete_network(NetworkId=net_id)


class TestMediaLiveVersions:
    """Tests for ListVersions."""

    def test_list_versions(self, medialive):
        resp = medialive.list_versions()
        assert "Versions" in resp
        assert isinstance(resp["Versions"], list)


class TestMediaLiveInputDeviceNotFound:
    """Tests for input device operations with nonexistent devices."""

    def test_describe_input_device_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_input_device(InputDeviceId="nonexistent-device")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_update_input_device_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.update_input_device(InputDeviceId="nonexistent-device")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_transfer_input_device_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.transfer_input_device(
                InputDeviceId="nonexistent-device",
                TargetCustomerId="123456789012",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_accept_input_device_transfer_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.accept_input_device_transfer(InputDeviceId="nonexistent-device")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_reject_input_device_transfer_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.reject_input_device_transfer(InputDeviceId="nonexistent-device")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_cancel_input_device_transfer_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.cancel_input_device_transfer(InputDeviceId="nonexistent-device")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveSignalMapMonitor:
    """Tests for signal map monitor deployment operations."""

    def test_start_monitor_deployment(self, medialive):
        resp = medialive.create_signal_map(
            DiscoveryEntryPointArn="arn:aws:medialive:us-east-1:123456789012:channel:1234",
            Name=_uid("sigmap"),
        )
        sig_id = resp["Id"]
        try:
            mon = medialive.start_monitor_deployment(Identifier=sig_id)
            assert "Id" in mon
            assert "MonitorDeployment" in mon
        finally:
            medialive.delete_signal_map(Identifier=sig_id)

    def test_start_delete_monitor_deployment(self, medialive):
        resp = medialive.create_signal_map(
            DiscoveryEntryPointArn="arn:aws:medialive:us-east-1:123456789012:channel:1234",
            Name=_uid("sigmap"),
        )
        sig_id = resp["Id"]
        try:
            medialive.start_monitor_deployment(Identifier=sig_id)
            del_mon = medialive.start_delete_monitor_deployment(Identifier=sig_id)
            assert "Id" in del_mon
            assert "MonitorDeployment" in del_mon
        finally:
            medialive.delete_signal_map(Identifier=sig_id)


class TestMediaLiveClaimDevice:
    """Tests for ClaimDevice."""

    def test_claim_device(self, medialive):
        resp = medialive.claim_device(Id=_uid("device"))
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMediaLiveNotFoundErrors:
    """Tests for not-found error handling on describe operations."""

    def test_describe_channel_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_channel(ChannelId="nonexistent-channel")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_input_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_input(InputId="nonexistent-input")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_multiplex_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_multiplex(MultiplexId="nonexistent-multiplex")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_network_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_network(NetworkId="nonexistent-network")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_cluster_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_cluster(ClusterId="nonexistent-cluster")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_signal_map_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.get_signal_map(Identifier="nonexistent-signal-map")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_cloud_watch_alarm_template_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.get_cloud_watch_alarm_template(Identifier="nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_event_bridge_rule_template_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.get_event_bridge_rule_template(Identifier="nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_cloud_watch_alarm_template_group_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.get_cloud_watch_alarm_template_group(Identifier="nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_event_bridge_rule_template_group_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.get_event_bridge_rule_template_group(Identifier="nonexistent")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveUpdateTemplateGroups:
    """Tests for updating template groups."""

    def test_update_cloud_watch_alarm_template_group(self, medialive):
        resp = medialive.create_cloud_watch_alarm_template_group(Name=_uid("cw-grp"))
        grp_id = resp["Id"]
        try:
            upd = medialive.update_cloud_watch_alarm_template_group(
                Identifier=grp_id,
                Description="Updated description",
            )
            assert upd["Description"] == "Updated description"
        finally:
            medialive.delete_cloud_watch_alarm_template_group(Identifier=grp_id)

    def test_update_event_bridge_rule_template_group(self, medialive):
        resp = medialive.create_event_bridge_rule_template_group(Name=_uid("eb-grp"))
        grp_id = resp["Id"]
        try:
            upd = medialive.update_event_bridge_rule_template_group(
                Identifier=grp_id,
                Description="Updated description",
            )
            assert upd["Description"] == "Updated description"
        finally:
            medialive.delete_event_bridge_rule_template_group(Identifier=grp_id)


class TestMediaLiveChannelPlacementGroups:
    """Tests for channel placement group operations."""

    def test_list_channel_placement_groups(self, medialive):
        try:
            resp = medialive.list_channel_placement_groups(ClusterId=_uid("cluster"))
            assert "ChannelPlacementGroups" in resp
        except ClientError as e:
            assert e.response["Error"]["Code"] == "NotFoundException"

    def test_describe_channel_placement_group_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_channel_placement_group(
                ClusterId=_uid("cluster"),
                ChannelPlacementGroupId=_uid("cpg"),
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_delete_channel_placement_group_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.delete_channel_placement_group(
                ClusterId=_uid("cluster"),
                ChannelPlacementGroupId=_uid("cpg"),
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_create_channel_placement_group_no_cluster(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.create_channel_placement_group(
                ClusterId=_uid("cluster"),
                Name=_uid("cpg"),
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_update_channel_placement_group_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.update_channel_placement_group(
                ClusterId=_uid("cluster"),
                ChannelPlacementGroupId=_uid("cpg"),
                Name=_uid("cpg-upd"),
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveSdiSources:
    """Tests for SDI source operations."""

    def test_list_sdi_sources(self, medialive):
        resp = medialive.list_sdi_sources()
        assert "SdiSources" in resp
        assert isinstance(resp["SdiSources"], list)

    def test_describe_sdi_source_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_sdi_source(SdiSourceId=_uid("sdi"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_delete_sdi_source_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.delete_sdi_source(SdiSourceId=_uid("sdi"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_create_sdi_source(self, medialive):
        resp = medialive.create_sdi_source(
            Name=_uid("sdi"),
            Type="SINGLE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_sdi_source_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.update_sdi_source(SdiSourceId=_uid("sdi"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveNodeAndClusterOps:
    """Tests for node registration, cluster alerts, and node state."""

    def test_create_node_registration_script(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.create_node_registration_script(
                ClusterId=_uid("cluster"),
                Id=_uid("node"),
                Name=_uid("node"),
                Role="BACKUP",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_list_cluster_alerts(self, medialive):
        try:
            resp = medialive.list_cluster_alerts(ClusterId=_uid("cluster"))
            assert "Alerts" in resp
        except ClientError as e:
            assert e.response["Error"]["Code"] == "NotFoundException"

    def test_update_node_state_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.update_node_state(
                ClusterId=_uid("cluster"),
                NodeId=_uid("node"),
                State="ACTIVE",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveInputDeviceOps:
    """Tests for input device operations."""

    def test_describe_input_device_thumbnail_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.describe_input_device_thumbnail(
                InputDeviceId=_uid("device"),
                Accept="image/jpeg",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_reboot_input_device_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.reboot_input_device(InputDeviceId=_uid("device"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_start_input_device_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.start_input_device(InputDeviceId=_uid("device"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_start_input_device_maintenance_window_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.start_input_device_maintenance_window(
                InputDeviceId=_uid("device"),
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_stop_input_device_not_found(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.stop_input_device(InputDeviceId=_uid("device"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestMediaLiveMultiplexAlerts:
    """Tests for multiplex alert operations."""

    def test_list_multiplex_alerts(self, medialive):
        with pytest.raises(ClientError) as exc:
            medialive.list_multiplex_alerts(MultiplexId=_uid("mpx"))
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
