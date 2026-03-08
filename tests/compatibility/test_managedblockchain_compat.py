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


class TestManagedBlockchainMemberOps:
    """Tests for Member operations."""

    @pytest.fixture
    def client(self):
        return make_client("managedblockchain")

    @pytest.fixture
    def network_with_member(self, client):
        """Create a network (which also creates the first member)."""
        resp = client.create_network(
            ClientRequestToken="test-token-net",
            Name="test-network",
            Framework="HYPERLEDGER_FABRIC",
            FrameworkVersion="1.2",
            FrameworkConfiguration={"Fabric": {"Edition": "STARTER"}},
            VotingPolicy={
                "ApprovalThresholdPolicy": {
                    "ThresholdPercentage": 50,
                    "ProposalDurationInHours": 24,
                    "ThresholdComparator": "GREATER_THAN",
                }
            },
            MemberConfiguration={
                "Name": "test-member",
                "Description": "Test member",
                "FrameworkConfiguration": {
                    "Fabric": {
                        "AdminUsername": "admin",
                        "AdminPassword": "Password123!",
                    }
                },
                "LogPublishingConfiguration": {
                    "Fabric": {"CaLogs": {"Cloudwatch": {"Enabled": False}}}
                },
            },
        )
        network_id = resp["NetworkId"]
        member_id = resp["MemberId"]
        yield {"NetworkId": network_id, "MemberId": member_id}

    def test_get_member(self, client, network_with_member):
        """GetMember returns details of a member."""
        resp = client.get_member(
            NetworkId=network_with_member["NetworkId"],
            MemberId=network_with_member["MemberId"],
        )
        assert "Member" in resp
        assert resp["Member"]["Id"] == network_with_member["MemberId"]
        assert resp["Member"]["Name"] == "test-member"

    def test_get_member_nonexistent(self, client, network_with_member):
        """GetMember with nonexistent member raises error."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_member(
                NetworkId=network_with_member["NetworkId"],
                MemberId="m-DOESNOTEXIST",
            )

    def test_delete_member(self, client, network_with_member):
        """DeleteMember removes a member."""
        resp = client.delete_member(
            NetworkId=network_with_member["NetworkId"],
            MemberId=network_with_member["MemberId"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_member(self, client, network_with_member):
        """CreateMember adds a new member to a network."""
        # First list invitations to get an invitation ID
        client.list_invitations()
        # CreateMember requires an InvitationId - we need a proposal first
        # Create a proposal to invite a new member
        proposal_resp = client.create_proposal(
            ClientRequestToken="test-proposal-token",
            NetworkId=network_with_member["NetworkId"],
            MemberId=network_with_member["MemberId"],
            Actions={
                "Invitations": [{"Principal": "123456789012"}],
            },
        )
        proposal_id = proposal_resp["ProposalId"]
        # Vote yes on the proposal
        client.vote_on_proposal(
            NetworkId=network_with_member["NetworkId"],
            ProposalId=proposal_id,
            VoterMemberId=network_with_member["MemberId"],
            Vote="YES",
        )
        # Now list invitations and find the PENDING one for our network
        inv_resp = client.list_invitations()
        network_id = network_with_member["NetworkId"]
        matching = [
            inv
            for inv in inv_resp["Invitations"]
            if inv["NetworkSummary"]["Id"] == network_id and inv["Status"] == "PENDING"
        ]
        assert len(matching) > 0
        invitation_id = matching[0]["InvitationId"]
        # Create the member
        resp = client.create_member(
            ClientRequestToken="test-member-token",
            InvitationId=invitation_id,
            NetworkId=network_with_member["NetworkId"],
            MemberConfiguration={
                "Name": "second-member",
                "Description": "Second member",
                "FrameworkConfiguration": {
                    "Fabric": {
                        "AdminUsername": "admin2",
                        "AdminPassword": "Password456!",
                    }
                },
                "LogPublishingConfiguration": {
                    "Fabric": {"CaLogs": {"Cloudwatch": {"Enabled": False}}}
                },
            },
        )
        assert "MemberId" in resp
        new_member_id = resp["MemberId"]
        # Verify it exists
        get_resp = client.get_member(
            NetworkId=network_with_member["NetworkId"],
            MemberId=new_member_id,
        )
        assert get_resp["Member"]["Name"] == "second-member"
