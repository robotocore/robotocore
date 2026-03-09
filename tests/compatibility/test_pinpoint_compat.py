"""Pinpoint compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def pinpoint():
    return make_client("pinpoint")


@pytest.fixture
def app_id(pinpoint):
    """Create a Pinpoint app and yield its ID; delete on teardown."""
    resp = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
    aid = resp["ApplicationResponse"]["Id"]
    yield aid
    pinpoint.delete_app(ApplicationId=aid)


@pytest.fixture
def segment_id(pinpoint, app_id):
    """Create a segment in the test app and yield its ID."""
    resp = pinpoint.create_segment(
        ApplicationId=app_id,
        WriteSegmentRequest={
            "Name": _unique("seg"),
            "Dimensions": {
                "Demographic": {"AppVersion": {"DimensionType": "INCLUSIVE", "Values": ["1.0"]}}
            },
        },
    )
    return resp["SegmentResponse"]["Id"]


class TestPinpointAppOperations:
    def test_create_app(self, pinpoint):
        name = _unique("app")
        resp = pinpoint.create_app(CreateApplicationRequest={"Name": name})
        app_resp = resp["ApplicationResponse"]
        assert app_resp["Name"] == name
        assert "Id" in app_resp
        assert "Arn" in app_resp
        # cleanup
        pinpoint.delete_app(ApplicationId=app_resp["Id"])

    def test_get_app(self, pinpoint):
        name = _unique("app")
        created = pinpoint.create_app(CreateApplicationRequest={"Name": name})
        app_id = created["ApplicationResponse"]["Id"]

        resp = pinpoint.get_app(ApplicationId=app_id)
        assert resp["ApplicationResponse"]["Id"] == app_id
        assert resp["ApplicationResponse"]["Name"] == name
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_get_app_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_app(ApplicationId="nonexistent-app-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_apps(self, pinpoint):
        name1 = _unique("app")
        name2 = _unique("app")
        resp1 = pinpoint.create_app(CreateApplicationRequest={"Name": name1})
        resp2 = pinpoint.create_app(CreateApplicationRequest={"Name": name2})
        app_id1 = resp1["ApplicationResponse"]["Id"]
        app_id2 = resp2["ApplicationResponse"]["Id"]

        resp = pinpoint.get_apps()
        items = resp["ApplicationsResponse"]["Item"]
        found_ids = [item["Id"] for item in items]
        assert app_id1 in found_ids
        assert app_id2 in found_ids
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id1)
        pinpoint.delete_app(ApplicationId=app_id2)

    def test_delete_app(self, pinpoint):
        name = _unique("app")
        created = pinpoint.create_app(CreateApplicationRequest={"Name": name})
        app_id = created["ApplicationResponse"]["Id"]

        resp = pinpoint.delete_app(ApplicationId=app_id)
        assert resp["ApplicationResponse"]["Id"] == app_id

        # Verify it no longer exists
        with pytest.raises(ClientError) as exc:
            pinpoint.get_app(ApplicationId=app_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_application_settings(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]

        resp = pinpoint.get_application_settings(ApplicationId=app_id)
        settings = resp["ApplicationSettingsResource"]
        assert settings["ApplicationId"] == app_id
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_update_application_settings(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]

        pinpoint.update_application_settings(
            ApplicationId=app_id,
            WriteApplicationSettingsRequest={"Limits": {"Daily": 200}},
        )
        resp = pinpoint.get_application_settings(ApplicationId=app_id)
        assert resp["ApplicationSettingsResource"]["Limits"]["Daily"] == 200
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_create_app_with_tags(self, pinpoint):
        name = _unique("app")
        resp = pinpoint.create_app(
            CreateApplicationRequest={"Name": name, "tags": {"env": "dev", "team": "qa"}}
        )
        app_id = resp["ApplicationResponse"]["Id"]
        arn = resp["ApplicationResponse"]["Arn"]
        try:
            # Verify tags were applied via list_tags_for_resource
            tags = pinpoint.list_tags_for_resource(ResourceArn=arn)["TagsModel"]["tags"]
            assert tags["env"] == "dev"
            assert tags["team"] == "qa"
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_delete_app_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.delete_app(ApplicationId="nonexistent-delete-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_update_application_settings_quiet_time(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.update_application_settings(
                ApplicationId=app_id,
                WriteApplicationSettingsRequest={"QuietTime": {"Start": "00:00", "End": "06:00"}},
            )
            resp = pinpoint.get_application_settings(ApplicationId=app_id)
            qt = resp["ApplicationSettingsResource"]["QuietTime"]
            assert qt["Start"] == "00:00"
            assert qt["End"] == "06:00"
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_update_application_settings_campaign_hook(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.update_application_settings(
                ApplicationId=app_id,
                WriteApplicationSettingsRequest={
                    "CampaignHook": {
                        "LambdaFunctionName": "my-hook-function",
                        "Mode": "DELIVERY",
                    }
                },
            )
            resp = pinpoint.get_application_settings(ApplicationId=app_id)
            hook = resp["ApplicationSettingsResource"]["CampaignHook"]
            assert hook["LambdaFunctionName"] == "my-hook-function"
            assert hook["Mode"] == "DELIVERY"
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_update_application_settings_full_limits(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.update_application_settings(
                ApplicationId=app_id,
                WriteApplicationSettingsRequest={
                    "Limits": {
                        "Daily": 50,
                        "MaximumDuration": 300,
                        "MessagesPerSecond": 25,
                        "Total": 500,
                    }
                },
            )
            resp = pinpoint.get_application_settings(ApplicationId=app_id)
            limits = resp["ApplicationSettingsResource"]["Limits"]
            assert limits["Daily"] == 50
            assert limits["MaximumDuration"] == 300
            assert limits["MessagesPerSecond"] == 25
            assert limits["Total"] == 500
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_get_application_settings_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_application_settings(ApplicationId="nonexistent-settings-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_apps_returns_list(self, pinpoint):
        """GetApps always returns an ApplicationsResponse with an Item list."""
        resp = pinpoint.get_apps()
        assert "ApplicationsResponse" in resp
        assert isinstance(resp["ApplicationsResponse"]["Item"], list)


class TestPinpointTagOperations:
    def test_tag_resource(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_resp = created["ApplicationResponse"]
        app_id = app_resp["Id"]
        arn = app_resp["Arn"]

        pinpoint.tag_resource(
            ResourceArn=arn, TagsModel={"tags": {"env": "test", "team": "backend"}}
        )
        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        tags = resp["TagsModel"]["tags"]
        assert tags["env"] == "test"
        assert tags["team"] == "backend"
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_list_tags_for_resource(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_resp = created["ApplicationResponse"]
        app_id = app_resp["Id"]
        arn = app_resp["Arn"]

        # Fresh app should have empty tags
        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        assert resp["TagsModel"]["tags"] == {}

        # Add tags and verify
        pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"key1": "val1"}})
        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        assert resp["TagsModel"]["tags"]["key1"] == "val1"
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_untag_resource(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_resp = created["ApplicationResponse"]
        app_id = app_resp["Id"]
        arn = app_resp["Arn"]

        pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"keep": "yes", "remove": "no"}})
        pinpoint.untag_resource(ResourceArn=arn, TagKeys=["remove"])

        resp = pinpoint.list_tags_for_resource(ResourceArn=arn)
        tags = resp["TagsModel"]["tags"]
        assert "keep" in tags
        assert "remove" not in tags
        # cleanup
        pinpoint.delete_app(ApplicationId=app_id)

    def test_tags_additive_across_calls(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        arn = created["ApplicationResponse"]["Arn"]
        try:
            pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"a": "1", "b": "2"}})
            pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"c": "3"}})
            tags = pinpoint.list_tags_for_resource(ResourceArn=arn)["TagsModel"]["tags"]
            assert tags["a"] == "1"
            assert tags["b"] == "2"
            assert tags["c"] == "3"
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_untag_multiple_keys(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        arn = created["ApplicationResponse"]["Arn"]
        try:
            pinpoint.tag_resource(
                ResourceArn=arn, TagsModel={"tags": {"a": "1", "b": "2", "c": "3"}}
            )
            pinpoint.untag_resource(ResourceArn=arn, TagKeys=["a", "c"])
            tags = pinpoint.list_tags_for_resource(ResourceArn=arn)["TagsModel"]["tags"]
            assert "a" not in tags
            assert tags["b"] == "2"
            assert "c" not in tags
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_tag_overwrite_value(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        arn = created["ApplicationResponse"]["Arn"]
        try:
            pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"key": "old"}})
            pinpoint.tag_resource(ResourceArn=arn, TagsModel={"tags": {"key": "new"}})
            tags = pinpoint.list_tags_for_resource(ResourceArn=arn)["TagsModel"]["tags"]
            assert tags["key"] == "new"
        finally:
            pinpoint.delete_app(ApplicationId=app_id)


class TestPinpointEventStreamOperations:
    def test_put_event_stream(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            resp = pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                },
            )
            es = resp["EventStream"]
            assert es["ApplicationId"] == app_id
            assert "DestinationStreamArn" in es
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_get_event_stream(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                },
            )
            resp = pinpoint.get_event_stream(ApplicationId=app_id)
            es = resp["EventStream"]
            assert es["ApplicationId"] == app_id
            assert (
                es["DestinationStreamArn"] == "arn:aws:kinesis:us-east-1:123456789012:stream/test"
            )
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_delete_event_stream(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                },
            )
            resp = pinpoint.delete_event_stream(ApplicationId=app_id)
            es = resp["EventStream"]
            assert es["ApplicationId"] == app_id
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_event_stream_update_overwrites(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/first",
                    "RoleArn": "arn:aws:iam::123456789012:role/role1",
                },
            )
            pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/second",
                    "RoleArn": "arn:aws:iam::123456789012:role/role2",
                },
            )
            resp = pinpoint.get_event_stream(ApplicationId=app_id)
            es = resp["EventStream"]
            assert (
                es["DestinationStreamArn"] == "arn:aws:kinesis:us-east-1:123456789012:stream/second"
            )
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_get_event_stream_after_delete_not_found(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
                    "RoleArn": "arn:aws:iam::123456789012:role/test-role",
                },
            )
            pinpoint.delete_event_stream(ApplicationId=app_id)
            with pytest.raises(ClientError) as exc:
                pinpoint.get_event_stream(ApplicationId=app_id)
            assert exc.value.response["Error"]["Code"] == "NotFoundException"
        finally:
            pinpoint.delete_app(ApplicationId=app_id)

    def test_put_event_stream_returns_role_arn(self, pinpoint):
        created = pinpoint.create_app(CreateApplicationRequest={"Name": _unique("app")})
        app_id = created["ApplicationResponse"]["Id"]
        try:
            resp = pinpoint.put_event_stream(
                ApplicationId=app_id,
                WriteEventStream={
                    "DestinationStreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
                    "RoleArn": "arn:aws:iam::123456789012:role/my-role",
                },
            )
            es = resp["EventStream"]
            assert es["RoleArn"] == "arn:aws:iam::123456789012:role/my-role"
            assert es["DestinationStreamArn"] == (
                "arn:aws:kinesis:us-east-1:123456789012:stream/test"
            )
        finally:
            pinpoint.delete_app(ApplicationId=app_id)


class TestPinpointChannelOperations:
    """Tests for Get*Channel operations - Moto returns default (disabled) channels."""

    def test_get_adm_channel(self, pinpoint, app_id):
        resp = pinpoint.get_adm_channel(ApplicationId=app_id)
        ch = resp["ADMChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "ADM"
        assert ch["Enabled"] is False

    def test_get_apns_channel(self, pinpoint, app_id):
        resp = pinpoint.get_apns_channel(ApplicationId=app_id)
        ch = resp["APNSChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "APNS"

    def test_get_apns_sandbox_channel(self, pinpoint, app_id):
        resp = pinpoint.get_apns_sandbox_channel(ApplicationId=app_id)
        ch = resp["APNSSandboxChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "APNS_SANDBOX"

    def test_get_apns_voip_channel(self, pinpoint, app_id):
        resp = pinpoint.get_apns_voip_channel(ApplicationId=app_id)
        ch = resp["APNSVoipChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "APNS_VOIP"

    def test_get_apns_voip_sandbox_channel(self, pinpoint, app_id):
        resp = pinpoint.get_apns_voip_sandbox_channel(ApplicationId=app_id)
        ch = resp["APNSVoipSandboxChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "APNS_VOIP_SANDBOX"

    def test_get_baidu_channel(self, pinpoint, app_id):
        resp = pinpoint.get_baidu_channel(ApplicationId=app_id)
        ch = resp["BaiduChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "BAIDU"

    def test_get_email_channel(self, pinpoint, app_id):
        resp = pinpoint.get_email_channel(ApplicationId=app_id)
        ch = resp["EmailChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "EMAIL"

    def test_get_gcm_channel(self, pinpoint, app_id):
        resp = pinpoint.get_gcm_channel(ApplicationId=app_id)
        ch = resp["GCMChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "GCM"

    def test_get_sms_channel(self, pinpoint, app_id):
        resp = pinpoint.get_sms_channel(ApplicationId=app_id)
        ch = resp["SMSChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "SMS"

    def test_get_voice_channel(self, pinpoint, app_id):
        resp = pinpoint.get_voice_channel(ApplicationId=app_id)
        ch = resp["VoiceChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "VOICE"

    def test_get_channels(self, pinpoint, app_id):
        resp = pinpoint.get_channels(ApplicationId=app_id)
        assert "ChannelsResponse" in resp
        channels = resp["ChannelsResponse"]["Channels"]
        assert isinstance(channels, dict)


class TestPinpointTemplateOperations:
    """Tests for Get*Template operations - return NotFoundException for nonexistent templates."""

    def test_get_email_template_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_email_template(TemplateName="nonexistent-email-tpl")
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "BadRequestException")

    def test_get_sms_template_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_sms_template(TemplateName="nonexistent-sms-tpl")
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "BadRequestException")

    def test_get_push_template_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_push_template(TemplateName="nonexistent-push-tpl")
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "BadRequestException")

    def test_get_voice_template_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_voice_template(TemplateName="nonexistent-voice-tpl")
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "BadRequestException")

    def test_get_in_app_template_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_in_app_template(TemplateName="nonexistent-inapp-tpl")
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "BadRequestException")

    def test_list_templates(self, pinpoint):
        resp = pinpoint.list_templates()
        assert "TemplatesResponse" in resp
        assert "Item" in resp["TemplatesResponse"]


class TestPinpointCampaignOperations:
    def test_get_campaigns_empty(self, pinpoint, app_id):
        resp = pinpoint.get_campaigns(ApplicationId=app_id)
        assert "CampaignsResponse" in resp
        assert "Item" in resp["CampaignsResponse"]

    def test_get_campaign_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_campaign(ApplicationId=app_id, CampaignId="nonexistent-campaign")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_campaign_activities_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_campaign_activities(
                ApplicationId=app_id, CampaignId="nonexistent-campaign"
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_campaign_versions_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_campaign_versions(ApplicationId=app_id, CampaignId="nonexistent-campaign")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestPinpointSegmentOperations:
    def test_get_segments(self, pinpoint, app_id):
        resp = pinpoint.get_segments(ApplicationId=app_id)
        assert "SegmentsResponse" in resp
        assert "Item" in resp["SegmentsResponse"]

    def test_get_segment(self, pinpoint, app_id, segment_id):
        resp = pinpoint.get_segment(ApplicationId=app_id, SegmentId=segment_id)
        assert resp["SegmentResponse"]["Id"] == segment_id
        assert "Name" in resp["SegmentResponse"]

    def test_get_segment_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_segment(ApplicationId=app_id, SegmentId="nonexistent-segment")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_segment_versions(self, pinpoint, app_id, segment_id):
        resp = pinpoint.get_segment_versions(ApplicationId=app_id, SegmentId=segment_id)
        assert "SegmentsResponse" in resp
        assert "Item" in resp["SegmentsResponse"]
        assert len(resp["SegmentsResponse"]["Item"]) >= 1

    def test_get_segment_export_jobs(self, pinpoint, app_id, segment_id):
        resp = pinpoint.get_segment_export_jobs(ApplicationId=app_id, SegmentId=segment_id)
        assert "ExportJobsResponse" in resp
        assert "Item" in resp["ExportJobsResponse"]

    def test_get_segment_import_jobs(self, pinpoint, app_id, segment_id):
        resp = pinpoint.get_segment_import_jobs(ApplicationId=app_id, SegmentId=segment_id)
        assert "ImportJobsResponse" in resp
        assert "Item" in resp["ImportJobsResponse"]


class TestPinpointJobOperations:
    def test_get_export_jobs(self, pinpoint, app_id):
        resp = pinpoint.get_export_jobs(ApplicationId=app_id)
        assert "ExportJobsResponse" in resp
        assert "Item" in resp["ExportJobsResponse"]

    def test_get_import_jobs(self, pinpoint, app_id):
        resp = pinpoint.get_import_jobs(ApplicationId=app_id)
        assert "ImportJobsResponse" in resp
        assert "Item" in resp["ImportJobsResponse"]

    def test_get_export_job_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_export_job(ApplicationId=app_id, JobId="nonexistent-job")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_import_job_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_import_job(ApplicationId=app_id, JobId="nonexistent-job")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestPinpointEndpointOperations:
    def test_get_endpoint_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_endpoint(ApplicationId=app_id, EndpointId="nonexistent-endpoint")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_user_endpoints(self, pinpoint, app_id):
        resp = pinpoint.get_user_endpoints(ApplicationId=app_id, UserId="nonexistent-user")
        assert "EndpointsResponse" in resp
        assert "Item" in resp["EndpointsResponse"]

    def test_get_in_app_messages(self, pinpoint, app_id):
        resp = pinpoint.get_in_app_messages(ApplicationId=app_id, EndpointId="fake-endpoint")
        assert "InAppMessagesResponse" in resp
        assert "InAppMessageCampaigns" in resp["InAppMessagesResponse"]


class TestPinpointJourneyOperations:
    def test_list_journeys(self, pinpoint, app_id):
        resp = pinpoint.list_journeys(ApplicationId=app_id)
        assert "JourneysResponse" in resp
        assert "Item" in resp["JourneysResponse"]

    def test_get_journey_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey(ApplicationId=app_id, JourneyId="nonexistent-journey")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestPinpointRecommenderOperations:
    def test_get_recommender_configurations(self, pinpoint):
        resp = pinpoint.get_recommender_configurations()
        assert "ListRecommenderConfigurationsResponse" in resp
        assert "Item" in resp["ListRecommenderConfigurationsResponse"]

    def test_get_recommender_configuration_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_recommender_configuration(RecommenderId="nonexistent-recommender")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestPinpointKpiOperations:
    def test_get_application_date_range_kpi(self, pinpoint, app_id):
        resp = pinpoint.get_application_date_range_kpi(
            ApplicationId=app_id, KpiName="successful-delivery-rate"
        )
        kpi = resp["ApplicationDateRangeKpiResponse"]
        assert kpi["ApplicationId"] == app_id
        assert kpi["KpiName"] == "successful-delivery-rate"
        assert "KpiResult" in kpi
        assert "StartTime" in kpi
        assert "EndTime" in kpi

    def test_get_campaign_date_range_kpi_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_campaign_date_range_kpi(
                ApplicationId=app_id,
                CampaignId="nonexistent-campaign",
                KpiName="successful-delivery-rate",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_journey_date_range_kpi_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey_date_range_kpi(
                ApplicationId=app_id,
                JourneyId="nonexistent-journey",
                KpiName="successful-delivery-rate",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestPinpointJourneyMetricOperations:
    def test_get_journey_execution_metrics_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey_execution_metrics(
                ApplicationId=app_id, JourneyId="nonexistent-journey"
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_journey_execution_activity_metrics_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey_execution_activity_metrics(
                ApplicationId=app_id,
                JourneyId="nonexistent-journey",
                JourneyActivityId="nonexistent-activity",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_journey_runs_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey_runs(ApplicationId=app_id, JourneyId="nonexistent-journey")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_journey_run_execution_metrics_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey_run_execution_metrics(
                ApplicationId=app_id,
                JourneyId="nonexistent-journey",
                RunId="nonexistent-run",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_journey_run_execution_activity_metrics_not_found(self, pinpoint, app_id):
        with pytest.raises(ClientError) as exc:
            pinpoint.get_journey_run_execution_activity_metrics(
                ApplicationId=app_id,
                JourneyId="nonexistent-journey",
                RunId="nonexistent-run",
                JourneyActivityId="nonexistent-activity",
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"


class TestPinpointTemplateVersionOperations:
    def test_list_template_versions_not_found(self, pinpoint):
        with pytest.raises(ClientError) as exc:
            pinpoint.list_template_versions(TemplateName="nonexistent", TemplateType="EMAIL")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
