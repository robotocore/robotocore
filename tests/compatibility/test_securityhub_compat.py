"""Security Hub compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def securityhub():
    return make_client("securityhub")


class TestSecurityHubOperations:
    def test_enable_describe_list_disable(self, securityhub):
        # Enable Security Hub
        enable_resp = securityhub.enable_security_hub(
            EnableDefaultStandards=False,
        )
        assert enable_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        try:
            # Describe hub
            describe_resp = securityhub.describe_hub()
            assert describe_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "HubArn" in describe_resp

            # List members
            members_resp = securityhub.list_members()
            assert members_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Members" in members_resp
        finally:
            # Disable Security Hub (may fail if parallel state reset occurred)
            try:
                securityhub.disable_security_hub()
            except Exception:
                pass


class TestSecurityhubAutoCoverage:
    """Auto-generated coverage tests for securityhub."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_administrator_account(self, client):
        """GetAdministratorAccount returns a response."""
        client.get_administrator_account()

    def test_get_findings(self, client):
        """GetFindings returns a response."""
        resp = client.get_findings()
        assert "Findings" in resp

    def test_get_master_account(self, client):
        """GetMasterAccount returns a response."""
        client.get_master_account()
