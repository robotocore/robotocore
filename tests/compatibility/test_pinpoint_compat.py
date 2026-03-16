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


class TestPinpointTemplateCRUD:
    """Tests for Create/Update/Delete template operations."""

    def test_create_email_template(self, pinpoint):
        name = _unique("email-tpl")
        resp = pinpoint.create_email_template(
            TemplateName=name,
            EmailTemplateRequest={"Subject": "Hello", "TextPart": "Body text"},
        )
        assert "CreateTemplateMessageBody" in resp
        # Verify it exists
        get_resp = pinpoint.get_email_template(TemplateName=name)
        assert get_resp["EmailTemplateResponse"]["TemplateName"] == name
        # Cleanup
        pinpoint.delete_email_template(TemplateName=name)

    def test_update_email_template(self, pinpoint):
        name = _unique("email-tpl")
        pinpoint.create_email_template(TemplateName=name, EmailTemplateRequest={"Subject": "Old"})
        resp = pinpoint.update_email_template(
            TemplateName=name, EmailTemplateRequest={"Subject": "New"}
        )
        assert "MessageBody" in resp
        pinpoint.delete_email_template(TemplateName=name)

    def test_delete_email_template(self, pinpoint):
        name = _unique("email-tpl")
        pinpoint.create_email_template(TemplateName=name, EmailTemplateRequest={"Subject": "Test"})
        resp = pinpoint.delete_email_template(TemplateName=name)
        assert "MessageBody" in resp
        with pytest.raises(ClientError):
            pinpoint.get_email_template(TemplateName=name)

    def test_create_sms_template(self, pinpoint):
        name = _unique("sms-tpl")
        resp = pinpoint.create_sms_template(
            TemplateName=name, SMSTemplateRequest={"Body": "Hello {{name}}"}
        )
        assert "CreateTemplateMessageBody" in resp
        get_resp = pinpoint.get_sms_template(TemplateName=name)
        assert get_resp["SMSTemplateResponse"]["TemplateName"] == name
        pinpoint.delete_sms_template(TemplateName=name)

    def test_update_sms_template(self, pinpoint):
        name = _unique("sms-tpl")
        pinpoint.create_sms_template(TemplateName=name, SMSTemplateRequest={"Body": "Old"})
        resp = pinpoint.update_sms_template(TemplateName=name, SMSTemplateRequest={"Body": "New"})
        assert "MessageBody" in resp
        pinpoint.delete_sms_template(TemplateName=name)

    def test_delete_sms_template(self, pinpoint):
        name = _unique("sms-tpl")
        pinpoint.create_sms_template(TemplateName=name, SMSTemplateRequest={"Body": "Test"})
        resp = pinpoint.delete_sms_template(TemplateName=name)
        assert "MessageBody" in resp

    def test_create_push_template(self, pinpoint):
        name = _unique("push-tpl")
        resp = pinpoint.create_push_template(
            TemplateName=name, PushNotificationTemplateRequest={"Default": {"Body": "Push!"}}
        )
        assert "CreateTemplateMessageBody" in resp
        get_resp = pinpoint.get_push_template(TemplateName=name)
        assert get_resp["PushNotificationTemplateResponse"]["TemplateName"] == name
        pinpoint.delete_push_template(TemplateName=name)

    def test_update_push_template(self, pinpoint):
        name = _unique("push-tpl")
        pinpoint.create_push_template(TemplateName=name, PushNotificationTemplateRequest={})
        resp = pinpoint.update_push_template(
            TemplateName=name, PushNotificationTemplateRequest={"Default": {"Body": "Updated"}}
        )
        assert "MessageBody" in resp
        pinpoint.delete_push_template(TemplateName=name)

    def test_delete_push_template(self, pinpoint):
        name = _unique("push-tpl")
        pinpoint.create_push_template(TemplateName=name, PushNotificationTemplateRequest={})
        resp = pinpoint.delete_push_template(TemplateName=name)
        assert "MessageBody" in resp

    def test_create_voice_template(self, pinpoint):
        name = _unique("voice-tpl")
        resp = pinpoint.create_voice_template(
            TemplateName=name, VoiceTemplateRequest={"Body": "Hello voice"}
        )
        assert "CreateTemplateMessageBody" in resp
        get_resp = pinpoint.get_voice_template(TemplateName=name)
        assert get_resp["VoiceTemplateResponse"]["TemplateName"] == name
        pinpoint.delete_voice_template(TemplateName=name)

    def test_update_voice_template(self, pinpoint):
        name = _unique("voice-tpl")
        pinpoint.create_voice_template(TemplateName=name, VoiceTemplateRequest={"Body": "Old"})
        resp = pinpoint.update_voice_template(
            TemplateName=name, VoiceTemplateRequest={"Body": "New"}
        )
        assert "MessageBody" in resp
        pinpoint.delete_voice_template(TemplateName=name)

    def test_delete_voice_template(self, pinpoint):
        name = _unique("voice-tpl")
        pinpoint.create_voice_template(TemplateName=name, VoiceTemplateRequest={"Body": "Test"})
        resp = pinpoint.delete_voice_template(TemplateName=name)
        assert "MessageBody" in resp

    def test_create_in_app_template(self, pinpoint):
        name = _unique("inapp-tpl")
        resp = pinpoint.create_in_app_template(TemplateName=name, InAppTemplateRequest={})
        assert "TemplateCreateMessageBody" in resp
        get_resp = pinpoint.get_in_app_template(TemplateName=name)
        assert get_resp["InAppTemplateResponse"]["TemplateName"] == name
        pinpoint.delete_in_app_template(TemplateName=name)

    def test_update_in_app_template(self, pinpoint):
        name = _unique("inapp-tpl")
        pinpoint.create_in_app_template(TemplateName=name, InAppTemplateRequest={})
        resp = pinpoint.update_in_app_template(TemplateName=name, InAppTemplateRequest={})
        assert "MessageBody" in resp
        pinpoint.delete_in_app_template(TemplateName=name)

    def test_delete_in_app_template(self, pinpoint):
        name = _unique("inapp-tpl")
        pinpoint.create_in_app_template(TemplateName=name, InAppTemplateRequest={})
        resp = pinpoint.delete_in_app_template(TemplateName=name)
        assert "MessageBody" in resp

    def test_update_template_active_version(self, pinpoint):
        name = _unique("email-tpl")
        pinpoint.create_email_template(TemplateName=name, EmailTemplateRequest={"Subject": "v1"})
        try:
            resp = pinpoint.update_template_active_version(
                TemplateName=name,
                TemplateType="EMAIL",
                TemplateActiveVersionRequest={"Version": "1"},
            )
            assert "MessageBody" in resp
        finally:
            pinpoint.delete_email_template(TemplateName=name)


class TestPinpointChannelDeleteOperations:
    """Tests for Delete*Channel operations."""

    def test_delete_email_channel(self, pinpoint, app_id):
        # Update first to ensure channel exists, then delete
        pinpoint.update_email_channel(
            ApplicationId=app_id,
            EmailChannelRequest={
                "FromAddress": "test@example.com",
                "Identity": "arn:aws:ses:us-east-1:123456789012:identity/example.com",
            },
        )
        resp = pinpoint.delete_email_channel(ApplicationId=app_id)
        assert resp["EmailChannelResponse"]["ApplicationId"] == app_id

    def test_delete_sms_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_sms_channel(ApplicationId=app_id)
        assert resp["SMSChannelResponse"]["ApplicationId"] == app_id

    def test_delete_voice_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_voice_channel(ApplicationId=app_id)
        assert resp["VoiceChannelResponse"]["ApplicationId"] == app_id

    def test_delete_adm_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_adm_channel(ApplicationId=app_id)
        assert resp["ADMChannelResponse"]["ApplicationId"] == app_id

    def test_delete_apns_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_apns_channel(ApplicationId=app_id)
        assert resp["APNSChannelResponse"]["ApplicationId"] == app_id

    def test_delete_apns_sandbox_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_apns_sandbox_channel(ApplicationId=app_id)
        assert resp["APNSSandboxChannelResponse"]["ApplicationId"] == app_id

    def test_delete_apns_voip_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_apns_voip_channel(ApplicationId=app_id)
        assert resp["APNSVoipChannelResponse"]["ApplicationId"] == app_id

    def test_delete_apns_voip_sandbox_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_apns_voip_sandbox_channel(ApplicationId=app_id)
        assert resp["APNSVoipSandboxChannelResponse"]["ApplicationId"] == app_id

    def test_delete_baidu_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_baidu_channel(ApplicationId=app_id)
        assert resp["BaiduChannelResponse"]["ApplicationId"] == app_id

    def test_delete_gcm_channel(self, pinpoint, app_id):
        resp = pinpoint.delete_gcm_channel(ApplicationId=app_id)
        assert resp["GCMChannelResponse"]["ApplicationId"] == app_id


class TestPinpointChannelUpdateOperations:
    """Tests for Update*Channel operations."""

    def test_update_email_channel(self, pinpoint, app_id):
        resp = pinpoint.update_email_channel(
            ApplicationId=app_id,
            EmailChannelRequest={
                "FromAddress": "test@example.com",
                "Identity": "arn:aws:ses:us-east-1:123456789012:identity/example.com",
            },
        )
        ch = resp["EmailChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "EMAIL"

    def test_update_sms_channel(self, pinpoint, app_id):
        resp = pinpoint.update_sms_channel(ApplicationId=app_id, SMSChannelRequest={})
        ch = resp["SMSChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "SMS"

    def test_update_voice_channel(self, pinpoint, app_id):
        resp = pinpoint.update_voice_channel(ApplicationId=app_id, VoiceChannelRequest={})
        ch = resp["VoiceChannelResponse"]
        assert ch["ApplicationId"] == app_id

    def test_update_adm_channel(self, pinpoint, app_id):
        resp = pinpoint.update_adm_channel(
            ApplicationId=app_id,
            ADMChannelRequest={"ClientId": "test-id", "ClientSecret": "test-secret"},
        )
        ch = resp["ADMChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "ADM"

    def test_update_apns_channel(self, pinpoint, app_id):
        resp = pinpoint.update_apns_channel(ApplicationId=app_id, APNSChannelRequest={})
        ch = resp["APNSChannelResponse"]
        assert ch["ApplicationId"] == app_id

    def test_update_apns_sandbox_channel(self, pinpoint, app_id):
        resp = pinpoint.update_apns_sandbox_channel(
            ApplicationId=app_id, APNSSandboxChannelRequest={}
        )
        ch = resp["APNSSandboxChannelResponse"]
        assert ch["ApplicationId"] == app_id

    def test_update_apns_voip_channel(self, pinpoint, app_id):
        resp = pinpoint.update_apns_voip_channel(ApplicationId=app_id, APNSVoipChannelRequest={})
        ch = resp["APNSVoipChannelResponse"]
        assert ch["ApplicationId"] == app_id

    def test_update_apns_voip_sandbox_channel(self, pinpoint, app_id):
        resp = pinpoint.update_apns_voip_sandbox_channel(
            ApplicationId=app_id, APNSVoipSandboxChannelRequest={}
        )
        ch = resp["APNSVoipSandboxChannelResponse"]
        assert ch["ApplicationId"] == app_id

    def test_update_baidu_channel(self, pinpoint, app_id):
        resp = pinpoint.update_baidu_channel(
            ApplicationId=app_id,
            BaiduChannelRequest={"ApiKey": "test-key", "SecretKey": "test-secret"},
        )
        ch = resp["BaiduChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "BAIDU"

    def test_update_gcm_channel(self, pinpoint, app_id):
        resp = pinpoint.update_gcm_channel(
            ApplicationId=app_id,
            GCMChannelRequest={"ApiKey": "test-gcm-key"},
        )
        ch = resp["GCMChannelResponse"]
        assert ch["ApplicationId"] == app_id
        assert ch["Platform"] == "GCM"


class TestPinpointCampaignCRUD:
    """Tests for Campaign create/update/delete and version operations."""

    @pytest.fixture
    def campaign_setup(self, pinpoint, app_id, segment_id):
        """Create a campaign and yield its data."""
        resp = pinpoint.create_campaign(
            ApplicationId=app_id,
            WriteCampaignRequest={
                "Name": _unique("camp"),
                "SegmentId": segment_id,
                "Schedule": {"StartTime": "IMMEDIATE"},
                "MessageConfiguration": {"DefaultMessage": {"Body": "hello"}},
            },
        )
        campaign_id = resp["CampaignResponse"]["Id"]
        yield {"campaign_id": campaign_id}
        try:
            pinpoint.delete_campaign(ApplicationId=app_id, CampaignId=campaign_id)
        except Exception:
            pass  # best-effort cleanup

    def test_create_campaign(self, pinpoint, app_id, segment_id):
        resp = pinpoint.create_campaign(
            ApplicationId=app_id,
            WriteCampaignRequest={
                "Name": _unique("camp"),
                "SegmentId": segment_id,
                "Schedule": {"StartTime": "IMMEDIATE"},
                "MessageConfiguration": {"DefaultMessage": {"Body": "hello"}},
            },
        )
        cr = resp["CampaignResponse"]
        assert cr["ApplicationId"] == app_id
        assert "Id" in cr
        assert cr["SegmentId"] == segment_id
        pinpoint.delete_campaign(ApplicationId=app_id, CampaignId=cr["Id"])

    def test_update_campaign(self, pinpoint, app_id, segment_id, campaign_setup):
        resp = pinpoint.update_campaign(
            ApplicationId=app_id,
            CampaignId=campaign_setup["campaign_id"],
            WriteCampaignRequest={
                "Name": "updated-camp",
                "SegmentId": segment_id,
                "Schedule": {"StartTime": "IMMEDIATE"},
            },
        )
        cr = resp["CampaignResponse"]
        assert cr["Id"] == campaign_setup["campaign_id"]
        assert cr["Name"] == "updated-camp"

    def test_delete_campaign(self, pinpoint, app_id, segment_id):
        resp = pinpoint.create_campaign(
            ApplicationId=app_id,
            WriteCampaignRequest={
                "Name": _unique("del-camp"),
                "SegmentId": segment_id,
                "Schedule": {"StartTime": "IMMEDIATE"},
                "MessageConfiguration": {"DefaultMessage": {"Body": "bye"}},
            },
        )
        cid = resp["CampaignResponse"]["Id"]
        del_resp = pinpoint.delete_campaign(ApplicationId=app_id, CampaignId=cid)
        assert del_resp["CampaignResponse"]["Id"] == cid

    def test_get_campaign_version(self, pinpoint, app_id, campaign_setup):
        resp = pinpoint.get_campaign_version(
            ApplicationId=app_id,
            CampaignId=campaign_setup["campaign_id"],
            Version="1",
        )
        assert resp["CampaignResponse"]["Id"] == campaign_setup["campaign_id"]


class TestPinpointSegmentCRUD:
    """Tests for segment update/delete and version operations."""

    def test_update_segment(self, pinpoint, app_id):
        seg = pinpoint.create_segment(
            ApplicationId=app_id,
            WriteSegmentRequest={
                "Name": _unique("seg"),
                "Dimensions": {
                    "Demographic": {"AppVersion": {"DimensionType": "INCLUSIVE", "Values": ["1.0"]}}
                },
            },
        )
        sid = seg["SegmentResponse"]["Id"]
        resp = pinpoint.update_segment(
            ApplicationId=app_id,
            SegmentId=sid,
            WriteSegmentRequest={
                "Name": "updated-seg",
                "Dimensions": {
                    "Demographic": {"AppVersion": {"DimensionType": "INCLUSIVE", "Values": ["2.0"]}}
                },
            },
        )
        assert resp["SegmentResponse"]["Id"] == sid
        assert resp["SegmentResponse"]["Name"] == "updated-seg"
        pinpoint.delete_segment(ApplicationId=app_id, SegmentId=sid)

    def test_delete_segment(self, pinpoint, app_id):
        seg = pinpoint.create_segment(
            ApplicationId=app_id,
            WriteSegmentRequest={
                "Name": _unique("del-seg"),
                "Dimensions": {
                    "Demographic": {"AppVersion": {"DimensionType": "INCLUSIVE", "Values": ["1.0"]}}
                },
            },
        )
        sid = seg["SegmentResponse"]["Id"]
        resp = pinpoint.delete_segment(ApplicationId=app_id, SegmentId=sid)
        assert resp["SegmentResponse"]["Id"] == sid

    def test_get_segment_version(self, pinpoint, app_id):
        seg = pinpoint.create_segment(
            ApplicationId=app_id,
            WriteSegmentRequest={
                "Name": _unique("ver-seg"),
                "Dimensions": {
                    "Demographic": {"AppVersion": {"DimensionType": "INCLUSIVE", "Values": ["1.0"]}}
                },
            },
        )
        sid = seg["SegmentResponse"]["Id"]
        resp = pinpoint.get_segment_version(ApplicationId=app_id, SegmentId=sid, Version="1")
        assert resp["SegmentResponse"]["Id"] == sid
        pinpoint.delete_segment(ApplicationId=app_id, SegmentId=sid)


class TestPinpointEndpointCRUD:
    """Tests for endpoint update/delete operations."""

    def test_update_endpoint(self, pinpoint, app_id):
        resp = pinpoint.update_endpoint(
            ApplicationId=app_id,
            EndpointId="test-ep-1",
            EndpointRequest={"Address": "test@example.com", "ChannelType": "EMAIL"},
        )
        assert "MessageBody" in resp
        # Verify it was created
        get_resp = pinpoint.get_endpoint(ApplicationId=app_id, EndpointId="test-ep-1")
        assert get_resp["EndpointResponse"]["Id"] == "test-ep-1"
        assert get_resp["EndpointResponse"]["Address"] == "test@example.com"

    def test_update_endpoints_batch(self, pinpoint, app_id):
        resp = pinpoint.update_endpoints_batch(
            ApplicationId=app_id,
            EndpointBatchRequest={
                "Item": [
                    {"Id": "batch-ep-1", "Address": "a@b.com", "ChannelType": "EMAIL"},
                    {"Id": "batch-ep-2", "Address": "c@d.com", "ChannelType": "EMAIL"},
                ]
            },
        )
        assert "MessageBody" in resp

    def test_delete_endpoint(self, pinpoint, app_id):
        # Create endpoint first
        pinpoint.update_endpoint(
            ApplicationId=app_id,
            EndpointId="del-ep",
            EndpointRequest={"Address": "del@test.com", "ChannelType": "EMAIL"},
        )
        resp = pinpoint.delete_endpoint(ApplicationId=app_id, EndpointId="del-ep")
        assert resp["EndpointResponse"]["Id"] == "del-ep"

    def test_delete_user_endpoints(self, pinpoint, app_id):
        # Create endpoint with a user ID
        pinpoint.update_endpoint(
            ApplicationId=app_id,
            EndpointId="user-ep",
            EndpointRequest={
                "Address": "user@test.com",
                "ChannelType": "EMAIL",
                "User": {"UserId": "test-user-123"},
            },
        )
        resp = pinpoint.delete_user_endpoints(ApplicationId=app_id, UserId="test-user-123")
        assert "EndpointsResponse" in resp


class TestPinpointJourneyCRUD:
    """Tests for journey create/update/delete operations."""

    def test_create_journey(self, pinpoint, app_id):
        resp = pinpoint.create_journey(
            ApplicationId=app_id,
            WriteJourneyRequest={
                "Name": _unique("journey"),
                "StartCondition": {"Description": "start"},
                "Schedule": {
                    "StartTime": "2025-01-01T00:00:00Z",
                    "EndTime": "2025-12-31T23:59:59Z",
                },
            },
        )
        jr = resp["JourneyResponse"]
        assert jr["ApplicationId"] == app_id
        assert "Id" in jr
        pinpoint.delete_journey(ApplicationId=app_id, JourneyId=jr["Id"])

    def test_update_journey(self, pinpoint, app_id):
        jr = pinpoint.create_journey(
            ApplicationId=app_id,
            WriteJourneyRequest={
                "Name": _unique("journey"),
                "StartCondition": {"Description": "start"},
            },
        )
        jid = jr["JourneyResponse"]["Id"]
        resp = pinpoint.update_journey(
            ApplicationId=app_id,
            JourneyId=jid,
            WriteJourneyRequest={"Name": "updated-journey"},
        )
        assert resp["JourneyResponse"]["Name"] == "updated-journey"
        pinpoint.delete_journey(ApplicationId=app_id, JourneyId=jid)

    def test_update_journey_state(self, pinpoint, app_id):
        jr = pinpoint.create_journey(
            ApplicationId=app_id,
            WriteJourneyRequest={
                "Name": _unique("journey"),
                "StartCondition": {"Description": "start"},
            },
        )
        jid = jr["JourneyResponse"]["Id"]
        resp = pinpoint.update_journey_state(
            ApplicationId=app_id,
            JourneyId=jid,
            JourneyStateRequest={"State": "CANCELLED"},
        )
        assert resp["JourneyResponse"]["Id"] == jid
        pinpoint.delete_journey(ApplicationId=app_id, JourneyId=jid)

    def test_delete_journey(self, pinpoint, app_id):
        jr = pinpoint.create_journey(
            ApplicationId=app_id,
            WriteJourneyRequest={
                "Name": _unique("del-journey"),
                "StartCondition": {"Description": "start"},
            },
        )
        jid = jr["JourneyResponse"]["Id"]
        resp = pinpoint.delete_journey(ApplicationId=app_id, JourneyId=jid)
        assert resp["JourneyResponse"]["Id"] == jid


class TestPinpointRecommenderCRUD:
    """Tests for recommender create/update/delete operations."""

    def test_create_recommender_configuration(self, pinpoint):
        resp = pinpoint.create_recommender_configuration(
            CreateRecommenderConfiguration={
                "RecommendationProviderUri": (
                    "arn:aws:personalize:us-east-1:123456789012:campaign/test"
                ),
                "RecommendationProviderRoleArn": ("arn:aws:iam::123456789012:role/test"),
            }
        )
        rec = resp["RecommenderConfigurationResponse"]
        assert "Id" in rec
        assert "RecommendationProviderUri" in rec
        pinpoint.delete_recommender_configuration(RecommenderId=rec["Id"])

    def test_update_recommender_configuration(self, pinpoint):
        resp = pinpoint.create_recommender_configuration(
            CreateRecommenderConfiguration={
                "RecommendationProviderUri": (
                    "arn:aws:personalize:us-east-1:123456789012:campaign/test"
                ),
                "RecommendationProviderRoleArn": ("arn:aws:iam::123456789012:role/test"),
            }
        )
        rec_id = resp["RecommenderConfigurationResponse"]["Id"]
        upd = pinpoint.update_recommender_configuration(
            RecommenderId=rec_id,
            UpdateRecommenderConfiguration={
                "RecommendationProviderUri": (
                    "arn:aws:personalize:us-east-1:123456789012:campaign/updated"
                ),
                "RecommendationProviderRoleArn": ("arn:aws:iam::123456789012:role/test"),
            },
        )
        assert "RecommenderConfigurationResponse" in upd
        pinpoint.delete_recommender_configuration(RecommenderId=rec_id)

    def test_delete_recommender_configuration(self, pinpoint):
        resp = pinpoint.create_recommender_configuration(
            CreateRecommenderConfiguration={
                "RecommendationProviderUri": (
                    "arn:aws:personalize:us-east-1:123456789012:campaign/del"
                ),
                "RecommendationProviderRoleArn": ("arn:aws:iam::123456789012:role/test"),
            }
        )
        rec_id = resp["RecommenderConfigurationResponse"]["Id"]
        del_resp = pinpoint.delete_recommender_configuration(RecommenderId=rec_id)
        assert "RecommenderConfigurationResponse" in del_resp


class TestPinpointJobCRUD:
    """Tests for export/import job creation."""

    def test_create_export_job(self, pinpoint, app_id):
        resp = pinpoint.create_export_job(
            ApplicationId=app_id,
            ExportJobRequest={
                "RoleArn": "arn:aws:iam::123456789012:role/test",
                "S3UrlPrefix": "s3://bucket/prefix",
            },
        )
        assert "ExportJobResponse" in resp
        assert resp["ExportJobResponse"]["ApplicationId"] == app_id
        assert "Id" in resp["ExportJobResponse"]

    def test_create_import_job(self, pinpoint, app_id):
        resp = pinpoint.create_import_job(
            ApplicationId=app_id,
            ImportJobRequest={
                "Format": "CSV",
                "RoleArn": "arn:aws:iam::123456789012:role/test",
                "S3Url": "s3://bucket/file.csv",
            },
        )
        assert "ImportJobResponse" in resp
        assert resp["ImportJobResponse"]["ApplicationId"] == app_id
        assert "Id" in resp["ImportJobResponse"]


class TestPinpointMessaging:
    """Tests for PutEvents, SendMessages, SendUsersMessages, etc."""

    def test_put_events(self, pinpoint, app_id):
        resp = pinpoint.put_events(
            ApplicationId=app_id,
            EventsRequest={
                "BatchItem": {
                    "ep1": {
                        "Endpoint": {},
                        "Events": {
                            "evt1": {
                                "EventType": "test-event",
                                "Timestamp": "2024-01-01T00:00:00Z",
                            }
                        },
                    }
                }
            },
        )
        assert "EventsResponse" in resp
        assert "Results" in resp["EventsResponse"]

    def test_send_messages(self, pinpoint, app_id):
        resp = pinpoint.send_messages(
            ApplicationId=app_id,
            MessageRequest={
                "MessageConfiguration": {"DefaultMessage": {"Body": "test"}},
                "Addresses": {"test@example.com": {"ChannelType": "EMAIL"}},
            },
        )
        assert "MessageResponse" in resp
        assert resp["MessageResponse"]["ApplicationId"] == app_id

    def test_send_users_messages(self, pinpoint, app_id):
        resp = pinpoint.send_users_messages(
            ApplicationId=app_id,
            SendUsersMessageRequest={
                "MessageConfiguration": {"DefaultMessage": {"Body": "test"}},
                "Users": {"user1": {}},
            },
        )
        assert "SendUsersMessageResponse" in resp
        assert resp["SendUsersMessageResponse"]["ApplicationId"] == app_id

    def test_phone_number_validate(self, pinpoint):
        resp = pinpoint.phone_number_validate(NumberValidateRequest={"PhoneNumber": "+12065551234"})
        assert "NumberValidateResponse" in resp

    def test_remove_attributes(self, pinpoint, app_id):
        resp = pinpoint.remove_attributes(
            ApplicationId=app_id,
            AttributeType="endpoint-custom-attributes",
            UpdateAttributesRequest={"Blacklist": ["attr1"]},
        )
        assert "AttributesResource" in resp
