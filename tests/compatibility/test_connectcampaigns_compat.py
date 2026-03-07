"""Connect Campaigns compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def connectcampaigns():
    return make_client("connectcampaigns")


class TestConnectCampaignsOperations:
    def test_list_campaigns(self, connectcampaigns):
        response = connectcampaigns.list_campaigns()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "campaignSummaryList" in response
