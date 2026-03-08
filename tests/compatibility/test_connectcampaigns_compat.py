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
