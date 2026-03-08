"""Managed Blockchain compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def managedblockchain():
    return make_client("managedblockchain")


class TestManagedBlockchainOperations:
    def test_list_networks(self, managedblockchain):
        response = managedblockchain.list_networks()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Networks" in response
        assert isinstance(response["Networks"], list)


class TestManagedblockchainAutoCoverage:
    """Auto-generated coverage tests for managedblockchain."""

    @pytest.fixture
    def client(self):
        return make_client("managedblockchain")

    def test_list_invitations(self, client):
        """ListInvitations returns a response."""
        resp = client.list_invitations()
        assert "Invitations" in resp
