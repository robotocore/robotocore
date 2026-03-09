"""Managed Blockchain compatibility tests."""

import uuid

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

    def test_list_members(self, client, network_with_member):
        """ListMembers returns the member we created."""
        resp = client.list_members(NetworkId=network_with_member["NetworkId"])
        assert "Members" in resp
        member_ids = [m["Id"] for m in resp["Members"]]
        assert network_with_member["MemberId"] in member_ids

    def test_update_member(self, client, network_with_member):
        """UpdateMember updates the member's log config."""
        resp = client.update_member(
            NetworkId=network_with_member["NetworkId"],
            MemberId=network_with_member["MemberId"],
            LogPublishingConfiguration={"Fabric": {"CaLogs": {"Cloudwatch": {"Enabled": True}}}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestManagedBlockchainNetworkOps:
    """Tests for Network operations."""

    @pytest.fixture
    def client(self):
        return make_client("managedblockchain")

    def _create_network(self, client):
        token = f"token-{uuid.uuid4().hex[:8]}"
        resp = client.create_network(
            ClientRequestToken=token,
            Name=f"net-{uuid.uuid4().hex[:8]}",
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
                "Name": f"member-{uuid.uuid4().hex[:8]}",
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
        return resp["NetworkId"], resp["MemberId"]

    def test_create_network(self, client):
        """CreateNetwork returns network and member IDs."""
        network_id, member_id = self._create_network(client)
        assert network_id
        assert member_id

    def test_get_network(self, client):
        """GetNetwork returns network details."""
        network_id, _ = self._create_network(client)
        resp = client.get_network(NetworkId=network_id)
        assert "Network" in resp
        assert resp["Network"]["Id"] == network_id
        assert resp["Network"]["Framework"] == "HYPERLEDGER_FABRIC"

    def test_list_networks_contains_created(self, client):
        """ListNetworks includes a newly created network."""
        network_id, _ = self._create_network(client)
        resp = client.list_networks()
        ids = [n["Id"] for n in resp["Networks"]]
        assert network_id in ids


class TestManagedBlockchainProposalOps:
    """Tests for Proposal operations."""

    @pytest.fixture
    def client(self):
        return make_client("managedblockchain")

    @pytest.fixture
    def network(self, client):
        token = f"token-{uuid.uuid4().hex[:8]}"
        resp = client.create_network(
            ClientRequestToken=token,
            Name=f"net-{uuid.uuid4().hex[:8]}",
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
                "Name": f"member-{uuid.uuid4().hex[:8]}",
                "Description": "Test",
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
        return {"NetworkId": resp["NetworkId"], "MemberId": resp["MemberId"]}

    def test_create_proposal(self, client, network):
        """CreateProposal creates a proposal for member invitation."""
        resp = client.create_proposal(
            ClientRequestToken=f"prop-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            Actions={
                "Invitations": [{"Principal": "123456789012"}],
            },
        )
        assert "ProposalId" in resp
        assert resp["ProposalId"]

    def test_list_proposals(self, client, network):
        """ListProposals returns proposals for a network."""
        client.create_proposal(
            ClientRequestToken=f"prop-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            Actions={"Invitations": [{"Principal": "123456789012"}]},
        )
        resp = client.list_proposals(NetworkId=network["NetworkId"])
        assert "Proposals" in resp
        assert len(resp["Proposals"]) >= 1

    def test_get_proposal(self, client, network):
        """GetProposal returns proposal details."""
        create_resp = client.create_proposal(
            ClientRequestToken=f"prop-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            Actions={"Invitations": [{"Principal": "123456789012"}]},
        )
        proposal_id = create_resp["ProposalId"]
        resp = client.get_proposal(
            NetworkId=network["NetworkId"],
            ProposalId=proposal_id,
        )
        assert "Proposal" in resp
        assert resp["Proposal"]["ProposalId"] == proposal_id

    def test_vote_on_proposal(self, client, network):
        """VoteOnProposal records a vote."""
        create_resp = client.create_proposal(
            ClientRequestToken=f"prop-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            Actions={"Invitations": [{"Principal": "123456789012"}]},
        )
        proposal_id = create_resp["ProposalId"]
        resp = client.vote_on_proposal(
            NetworkId=network["NetworkId"],
            ProposalId=proposal_id,
            VoterMemberId=network["MemberId"],
            Vote="YES",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_proposal_votes(self, client, network):
        """ListProposalVotes returns votes for a proposal."""
        create_resp = client.create_proposal(
            ClientRequestToken=f"prop-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            Actions={"Invitations": [{"Principal": "123456789012"}]},
        )
        proposal_id = create_resp["ProposalId"]
        client.vote_on_proposal(
            NetworkId=network["NetworkId"],
            ProposalId=proposal_id,
            VoterMemberId=network["MemberId"],
            Vote="YES",
        )
        resp = client.list_proposal_votes(
            NetworkId=network["NetworkId"],
            ProposalId=proposal_id,
        )
        assert "ProposalVotes" in resp
        assert len(resp["ProposalVotes"]) >= 1


class TestManagedBlockchainNodeOps:
    """Tests for Node operations."""

    @pytest.fixture
    def client(self):
        return make_client("managedblockchain")

    @pytest.fixture
    def network(self, client):
        token = f"token-{uuid.uuid4().hex[:8]}"
        resp = client.create_network(
            ClientRequestToken=token,
            Name=f"net-{uuid.uuid4().hex[:8]}",
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
                "Name": f"member-{uuid.uuid4().hex[:8]}",
                "Description": "Test",
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
        return {"NetworkId": resp["NetworkId"], "MemberId": resp["MemberId"]}

    def test_create_node(self, client, network):
        """CreateNode creates a peer node."""
        resp = client.create_node(
            ClientRequestToken=f"node-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            NodeConfiguration={
                "InstanceType": "bc.t3.small",
                "AvailabilityZone": "us-east-1a",
                "LogPublishingConfiguration": {
                    "Fabric": {
                        "ChaincodeLogs": {"Cloudwatch": {"Enabled": False}},
                        "PeerLogs": {"Cloudwatch": {"Enabled": False}},
                    }
                },
            },
        )
        assert "NodeId" in resp
        assert resp["NodeId"]

    def test_list_nodes(self, client, network):
        """ListNodes returns nodes for a member."""
        client.create_node(
            ClientRequestToken=f"node-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            NodeConfiguration={
                "InstanceType": "bc.t3.small",
                "AvailabilityZone": "us-east-1a",
                "LogPublishingConfiguration": {
                    "Fabric": {
                        "ChaincodeLogs": {"Cloudwatch": {"Enabled": False}},
                        "PeerLogs": {"Cloudwatch": {"Enabled": False}},
                    }
                },
            },
        )
        resp = client.list_nodes(
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
        )
        assert "Nodes" in resp
        assert len(resp["Nodes"]) >= 1

    def test_get_node(self, client, network):
        """GetNode returns node details."""
        create_resp = client.create_node(
            ClientRequestToken=f"node-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            NodeConfiguration={
                "InstanceType": "bc.t3.small",
                "AvailabilityZone": "us-east-1a",
                "LogPublishingConfiguration": {
                    "Fabric": {
                        "ChaincodeLogs": {"Cloudwatch": {"Enabled": False}},
                        "PeerLogs": {"Cloudwatch": {"Enabled": False}},
                    }
                },
            },
        )
        node_id = create_resp["NodeId"]
        resp = client.get_node(
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            NodeId=node_id,
        )
        assert "Node" in resp
        assert resp["Node"]["Id"] == node_id

    def test_delete_node(self, client, network):
        """DeleteNode removes a node."""
        create_resp = client.create_node(
            ClientRequestToken=f"node-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            NodeConfiguration={
                "InstanceType": "bc.t3.small",
                "AvailabilityZone": "us-east-1a",
                "LogPublishingConfiguration": {
                    "Fabric": {
                        "ChaincodeLogs": {"Cloudwatch": {"Enabled": False}},
                        "PeerLogs": {"Cloudwatch": {"Enabled": False}},
                    }
                },
            },
        )
        node_id = create_resp["NodeId"]
        resp = client.delete_node(
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            NodeId=node_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestManagedBlockchainAdditional:
    """Additional tests for Managed Blockchain operations."""

    @pytest.fixture
    def client(self):
        return make_client("managedblockchain")

    @pytest.fixture
    def network(self, client):
        token = f"token-{uuid.uuid4().hex[:8]}"
        resp = client.create_network(
            ClientRequestToken=token,
            Name=f"net-{uuid.uuid4().hex[:8]}",
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
                "Name": f"member-{uuid.uuid4().hex[:8]}",
                "Description": "Test",
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
        return {"NetworkId": resp["NetworkId"], "MemberId": resp["MemberId"]}

    def test_create_member(self, client, network):
        """CreateMember adds a new member to an existing network via proposal invitation."""
        # Create a proposal to invite a new member
        prop_resp = client.create_proposal(
            ClientRequestToken=f"prop-{uuid.uuid4().hex[:8]}",
            NetworkId=network["NetworkId"],
            MemberId=network["MemberId"],
            Actions={"Invitations": [{"Principal": "123456789012"}]},
        )
        proposal_id = prop_resp["ProposalId"]
        # Vote YES to approve
        client.vote_on_proposal(
            NetworkId=network["NetworkId"],
            ProposalId=proposal_id,
            VoterMemberId=network["MemberId"],
            Vote="YES",
        )
        # Now create the member using the invitation
        invitations = client.list_invitations()["Invitations"]
        # Find the invitation for our network
        invitation_id = None
        for inv in invitations:
            if inv["NetworkSummary"]["Id"] == network["NetworkId"]:
                invitation_id = inv["InvitationId"]
                break
        assert invitation_id is not None, "Expected an invitation for the network"
        # Create a member on the network
        resp = client.create_member(
            ClientRequestToken=f"mem-{uuid.uuid4().hex[:8]}",
            InvitationId=invitation_id,
            NetworkId=network["NetworkId"],
            MemberConfiguration={
                "Name": f"new-member-{uuid.uuid4().hex[:8]}",
                "Description": "New member via invitation",
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
        assert resp["MemberId"]

    def test_reject_invitation_nonexistent(self, client):
        """RejectInvitation with a nonexistent invitation ID raises error."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.reject_invitation(InvitationId="inv-doesnotexist12345")
