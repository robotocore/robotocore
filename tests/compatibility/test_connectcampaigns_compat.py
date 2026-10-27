"""Connect Campaigns compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def connectcampaigns():
    return make_client("connectcampaigns")


def _create_campaign(client, name="test-campaign"):
    """Helper to create a campaign and return its ID."""
    return client.create_campaign(
        name=name,
        connectInstanceId="12345678-1234-1234-1234-123456789012",
        dialerConfig={"progressiveDialerConfig": {"bandwidthAllocation": 1.0}},
        outboundCallConfig={
            "connectContactFlowId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "connectQueueId": "aaaaaaaa-bbbb-cccc-dddd-ffffffffffff",
        },
    )


class TestConnectCampaignsOperations:
    def test_list_campaigns(self, connectcampaigns):
        response = connectcampaigns.list_campaigns()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "campaignSummaryList" in response

    def test_describe_campaign(self, connectcampaigns):
        resp = _create_campaign(connectcampaigns)
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.describe_campaign(id=campaign_id)
            assert "campaign" in result
            assert result["campaign"]["id"] == campaign_id
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_describe_campaign_nonexistent(self, connectcampaigns):
        with pytest.raises(ClientError) as exc:
            connectcampaigns.describe_campaign(id="nonexistent-campaign-id")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidCampaignStateException",
        )

    def test_start_campaign(self, connectcampaigns):
        resp = _create_campaign(connectcampaigns, name="start-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.start_campaign(id=campaign_id)
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_stop_campaign(self, connectcampaigns):
        resp = _create_campaign(connectcampaigns, name="stop-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.stop_campaign(id=campaign_id)
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_get_campaign_state(self, connectcampaigns):
        """GetCampaignState returns the state of a campaign."""
        resp = _create_campaign(connectcampaigns, name="state-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.get_campaign_state(id=campaign_id)
            assert "state" in result
            assert result["state"] in (
                "Initialized",
                "Running",
                "Paused",
                "Stopped",
                "Failed",
            )
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_get_campaign_state_nonexistent(self, connectcampaigns):
        """GetCampaignState for nonexistent campaign raises error."""
        with pytest.raises(ClientError) as exc:
            connectcampaigns.get_campaign_state(id="nonexistent-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_pause_campaign(self, connectcampaigns):
        """PauseCampaign returns 200."""
        resp = _create_campaign(connectcampaigns, name="pause-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.pause_campaign(id=campaign_id)
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_resume_campaign(self, connectcampaigns):
        """ResumeCampaign returns 200."""
        resp = _create_campaign(connectcampaigns, name="resume-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.resume_campaign(id=campaign_id)
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_tag_resource(self, connectcampaigns):
        """TagResource adds tags to a campaign."""
        resp = _create_campaign(connectcampaigns, name="tag-test")
        campaign_id = resp["id"]
        try:
            desc = connectcampaigns.describe_campaign(id=campaign_id)
            arn = desc["campaign"]["arn"]
            connectcampaigns.tag_resource(arn=arn, tags={"env": "test", "team": "dev"})
            tags = connectcampaigns.list_tags_for_resource(arn=arn)
            assert tags["tags"]["env"] == "test"
            assert tags["tags"]["team"] == "dev"
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_list_tags_for_resource(self, connectcampaigns):
        """ListTagsForResource returns tags dict."""
        resp = _create_campaign(connectcampaigns, name="listtag-test")
        campaign_id = resp["id"]
        try:
            desc = connectcampaigns.describe_campaign(id=campaign_id)
            arn = desc["campaign"]["arn"]
            result = connectcampaigns.list_tags_for_resource(arn=arn)
            assert "tags" in result
            assert isinstance(result["tags"], dict)
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_untag_resource(self, connectcampaigns):
        """UntagResource removes a specific tag key."""
        resp = _create_campaign(connectcampaigns, name="untag-test")
        campaign_id = resp["id"]
        try:
            desc = connectcampaigns.describe_campaign(id=campaign_id)
            arn = desc["campaign"]["arn"]
            connectcampaigns.tag_resource(arn=arn, tags={"k1": "v1", "k2": "v2"})
            connectcampaigns.untag_resource(arn=arn, tagKeys=["k1"])
            tags = connectcampaigns.list_tags_for_resource(arn=arn)
            assert "k1" not in tags["tags"]
            assert tags["tags"]["k2"] == "v2"
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_get_connect_instance_config(self, connectcampaigns):
        """GetConnectInstanceConfig returns instance configuration."""
        # First ensure an instance config exists by creating a campaign
        resp = _create_campaign(connectcampaigns, name="config-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.get_connect_instance_config(
                connectInstanceId="12345678-1234-1234-1234-123456789012"
            )
            assert "connectInstanceConfig" in result
            config = result["connectInstanceConfig"]
            assert config["connectInstanceId"] == "12345678-1234-1234-1234-123456789012"
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_create_and_delete_campaign(self, connectcampaigns):
        """CreateCampaign and DeleteCampaign lifecycle."""
        resp = _create_campaign(connectcampaigns, name="lifecycle-test")
        campaign_id = resp["id"]
        assert "arn" in resp
        assert len(campaign_id) > 0
        # Delete and verify describe fails
        connectcampaigns.delete_campaign(id=campaign_id)
        with pytest.raises(ClientError) as exc:
            connectcampaigns.describe_campaign(id=campaign_id)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "InvalidCampaignStateException",
        )

    def test_describe_campaign_returns_full_details(self, connectcampaigns):
        """DescribeCampaign returns all expected fields."""
        resp = _create_campaign(connectcampaigns, name="details-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.describe_campaign(id=campaign_id)
            campaign = result["campaign"]
            assert campaign["id"] == campaign_id
            assert "name" in campaign
            assert "arn" in campaign
            assert "connectInstanceId" in campaign
            assert "dialerConfig" in campaign
            assert "outboundCallConfig" in campaign
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_list_campaigns_includes_created(self, connectcampaigns):
        """ListCampaigns includes a newly created campaign."""
        resp = _create_campaign(connectcampaigns, name="list-include-test")
        campaign_id = resp["id"]
        try:
            result = connectcampaigns.list_campaigns()
            campaign_ids = [c["id"] for c in result["campaignSummaryList"]]
            assert campaign_id in campaign_ids
        finally:
            connectcampaigns.delete_campaign(id=campaign_id)

    def test_start_instance_onboarding_job(self, connectcampaigns):
        """StartInstanceOnboardingJob returns job status."""
        result = connectcampaigns.start_instance_onboarding_job(
            connectInstanceId="12345678-1234-1234-1234-123456789012",
            encryptionConfig={"enabled": False, "encryptionType": "KMS"},
        )
        assert "connectInstanceOnboardingJobStatus" in result
        status = result["connectInstanceOnboardingJobStatus"]
        assert status["connectInstanceId"] == "12345678-1234-1234-1234-123456789012"
        assert status["status"] in ("IN_PROGRESS", "SUCCEEDED", "FAILED")
